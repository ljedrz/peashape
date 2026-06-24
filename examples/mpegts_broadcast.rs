//! Masquerading `peashape` *broadcast* traffic as a live, scrambled
//! **MPEG transport stream** (IPTV-style) fanned out to many viewers.
//!
//! Where `webrtc_masquerade` shapes a single 1:1 link
//! (`ShapingScope::PerConnection`), this example uses
//! `ShapingScope::Global` — peashape's one-to-many fan-out — to mimic
//! the canonical broadcast medium: a constant-bitrate MPEG-TS stream,
//! the format carried by DVB, IPTV, HLS segments, and SRT contribution
//! feeds.
//!
//! # Why MPEG-TS is a perfect fit
//!
//! An MPEG transport stream is, by definition, a sequence of
//! **fixed 188-byte packets** — which is exactly peashape's constant
//! frame size. Each packet has a 4-byte cleartext header:
//!
//! ```text
//!   0x47 │ PUSI + PID (13b) │ scrambling + CC (4b) │  184-byte payload
//!   sync │  elementary stream id │  continuity counter │  (scrambled)
//! ```
//!
//! - the **sync byte** `0x47` starts every packet,
//! - a fixed **PID** identifies the elementary stream (one "channel"),
//! - the **transport_scrambling_control** bits mark the payload as
//!   scrambled, so an observer legitimately sees random-looking
//!   payload bytes (real scrambled TS looks the same), and
//! - the 4-bit **continuity counter** increments per packet of a PID
//!   and wraps mod 16 — the stream's natural sequencing field.
//!
//! One source sending identical packets to every viewer is exactly how
//! multicast TV works, so `Global` scope with `fanout` ≥ the number of
//! viewers reproduces it faithfully: every tick emits one TS packet to
//! every viewer, and each viewer reassembles a clean stream with a
//! gap-free continuity counter.
//!
//! # The cover generator muxes real data into the stream
//!
//! As in the WebRTC example, a custom [`CoverGenerator`] owns the
//! stream's sequencing. Its two hooks share one continuity counter:
//!
//! - [`cover`](CoverGenerator::cover) emits a scrambled-noise TS packet
//!   on empty ticks, and
//! - [`finalize_real`](CoverGenerator::finalize_real) stamps the TS
//!   header (sync, PID, scrambling, the next CC, and the
//!   payload-unit-start flag) onto a real frame drained from the
//!   priority lane.
//!
//! So real messages ride peashape's high-priority lane and are muxed
//! inline with the comfort packets under one continuity counter; every
//! viewer sees the same coherent, gap-free TS stream.
//!
//! Run with: cargo run --example mpegts_broadcast

use std::collections::HashSet;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use rand::Rng;
use tokio::sync::broadcast::error::TryRecvError;

use peashape::{CoverGenerator, Lane, Node, ShapeConfig, ShapingScope, ShapingStrategy, Target};

// --- MPEG-TS wire profile --------------------------------------------------

/// The MPEG-TS sync byte that starts every 188-byte packet.
const TS_SYNC: u8 = 0x47;
/// A transport-stream packet is always exactly 188 bytes.
const TS_PACKET: usize = 188;
/// Bytes in the (minimal, adaptation-free) TS header.
const TS_HEADER: usize = 4;
/// Payload bytes per packet (188 − 4).
const TS_PAYLOAD: usize = TS_PACKET - TS_HEADER;
/// The PID of our single elementary stream ("channel"). 0x0100 is a
/// typical video PID.
const VIDEO_PID: u16 = 0x0100;
/// Inter-packet interval. One packet per 10 ms ⇒ 100 pps ⇒ ~150 kbps,
/// a plausible low-bitrate scrambled stream.
const TS_INTERVAL: Duration = Duration::from_millis(10);
/// Shared secret used by the toy keystream (see `keystream`).
const KEY: u32 = 0x5a5a_5a5a;
/// Bytes of random per-packet nonce at the front of the payload.
const NONCE: usize = 4;

/// A 4-byte marker at the start of a real packet's *descrambled*
/// payload, so a viewer can tell real data from comfort noise.
const REAL_MAGIC: &[u8; 4] = b"TS!!";

/// A toy keystream standing in for MPEG-TS scrambling (DVB-CSA). Keyed
/// on a per-packet random `nonce` carried in the payload, so identical
/// plaintext never produces identical ciphertext and the header can be
/// stamped at egress without disturbing descrambling. **Not** real
/// crypto — in production the DTLS-SRTP keys from a `pea2pea`
/// `Handshake` (or a real conditional-access system) would do this.
fn keystream(nonce: u32, i: usize) -> u8 {
    let n = nonce.wrapping_add(i as u32).wrapping_mul(2_654_435_761);
    (KEY ^ n ^ (n >> 13)) as u8
}

/// Builds one 188-byte TS packet with a *zeroed* header (the muxer
/// stamps it at egress): `header || nonce || scrambled body`. If
/// `message` is `Some`, the packet carries real data; otherwise it is
/// scrambled comfort noise.
fn build_ts_packet(message: Option<&[u8]>) -> BytesMut {
    let mut f = BytesMut::with_capacity(TS_PACKET);
    let mut rng = rand::rng();

    f.put_bytes(0, TS_HEADER); // placeholder header, stamped at egress

    let mut payload = vec![0u8; TS_PAYLOAD];
    rng.fill(&mut payload[..]);
    if let Some(msg) = message {
        let nonce = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let take = msg.len().min(TS_PAYLOAD - NONCE - REAL_MAGIC.len() - 1);
        let mut pt = Vec::with_capacity(REAL_MAGIC.len() + 1 + take);
        pt.extend_from_slice(REAL_MAGIC);
        pt.push(take as u8);
        pt.extend_from_slice(&msg[..take]);
        for (i, b) in pt.iter().enumerate() {
            payload[NONCE + i] = b ^ keystream(nonce, i);
        }
    }
    f.extend_from_slice(&payload);

    debug_assert_eq!(f.len(), TS_PACKET);
    f
}

/// Parses the cleartext TS header. Returns `None` for a non-TS frame.
/// Returns `(pid, continuity_counter, payload_unit_start, scrambling)`.
fn parse_ts(f: &[u8]) -> Option<(u16, u8, bool, u8)> {
    if f.len() < TS_HEADER || f[0] != TS_SYNC {
        return None;
    }
    let pusi = f[1] & 0x40 != 0;
    let pid = (((f[1] & 0x1f) as u16) << 8) | f[2] as u16;
    let scrambling = f[3] >> 6;
    let cc = f[3] & 0x0f;
    Some((pid, cc, pusi, scrambling))
}

/// Descrambles a real message out of a TS packet, or `None` if it is
/// comfort noise. Reads the per-packet nonce from the payload.
fn recover(frame: &[u8]) -> Option<Vec<u8>> {
    let payload = frame.get(TS_HEADER..TS_PACKET)?;
    let nonce = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let body = &payload[NONCE..];
    let mut pt = vec![0u8; body.len()];
    for (i, b) in body.iter().enumerate() {
        pt[i] = b ^ keystream(nonce, i);
    }
    if &pt[..REAL_MAGIC.len()] != REAL_MAGIC {
        return None;
    }
    let len = pt[REAL_MAGIC.len()] as usize;
    let start = REAL_MAGIC.len() + 1;
    pt.get(start..start + len).map(<[u8]>::to_vec)
}

// --- the cover generator: one continuity counter for the whole stream ------

/// Muxes the transport stream: stamps *every* outgoing packet — comfort
/// noise via [`cover`](CoverGenerator::cover) and real lane frames via
/// [`finalize_real`](CoverGenerator::finalize_real) — from a single
/// continuity counter, so every viewer reassembles one gap-free stream.
struct TsMuxer {
    pid: u16,
    cc: AtomicU8,
}

impl TsMuxer {
    fn new(pid: u16) -> Self {
        Self {
            pid,
            cc: AtomicU8::new(0),
        }
    }

    /// Stamps the 4-byte TS header in place with the next continuity
    /// counter. `pusi` marks the start of a payload unit (set for real
    /// data, clear for comfort/continuation packets).
    fn stamp(&self, f: &mut [u8], pusi: bool) {
        let cc = self.cc.fetch_add(1, Ordering::Relaxed) & 0x0f;
        f[0] = TS_SYNC;
        f[1] = (u8::from(pusi) << 6) | ((self.pid >> 8) as u8 & 0x1f);
        f[2] = (self.pid & 0xff) as u8;
        // scrambling=0b11 (scrambled, even key) | adaptation=0b01
        // (payload only) | 4-bit continuity counter.
        f[3] = 0xd0 | cc;
    }
}

impl CoverGenerator for TsMuxer {
    fn cover(&self, _config: &ShapeConfig) -> BytesMut {
        let mut f = build_ts_packet(None);
        self.stamp(&mut f, false);
        f
    }

    fn finalize_real(&self, _config: &ShapeConfig, mut frame: BytesMut) -> BytesMut {
        self.stamp(&mut frame, true);
        frame
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const VIEWERS: usize = 3;

    println!("peashape — masquerading broadcast traffic as scrambled MPEG-TS (IPTV)\n");
    println!("profile: {TS_PACKET}-byte TS packets, PID 0x{VIDEO_PID:04x}, scrambled,",);
    println!(
        "         one every {} ms ({} pps, ~{} kbps), fanned to {VIEWERS} viewers\n",
        TS_INTERVAL.as_millis(),
        1000 / TS_INTERVAL.as_millis(),
        TS_PACKET * 8 * (1000 / TS_INTERVAL.as_millis() as usize) / 1000,
    );

    // The broadcaster fans one TS stream out to every viewer. Its cover
    // generator owns the continuity counter for both cover and real
    // (lane) packets.
    let mux: Arc<dyn CoverGenerator> = Arc::new(TsMuxer::new(VIDEO_PID));
    let broadcaster = Node::new(ShapeConfig {
        name: Some("broadcaster".into()),
        listener_addr: Some("127.0.0.1:0".parse()?),
        strategy: ShapingStrategy::Constant {
            interval: TS_INTERVAL,
        },
        scope: ShapingScope::Global,
        // fanout ≥ viewers ⇒ every packet reaches every viewer, so each
        // viewer's continuity counter is gap-free (multicast semantics).
        fanout: VIEWERS + 4,
        frame_size: TS_PACKET,
        cover_generator: Some(mux),
        ..Default::default()
    });

    let mut viewers = Vec::new();
    for i in 0..VIEWERS {
        let v = Node::new(ShapeConfig {
            name: Some(format!("viewer-{i}")),
            listener_addr: Some("127.0.0.1:0".parse()?),
            strategy: ShapingStrategy::Constant {
                interval: Duration::from_secs(10),
            },
            scope: ShapingScope::Global,
            frame_size: TS_PACKET,
            ..Default::default()
        });
        viewers.push(v);
    }

    broadcaster.spawn().await?;
    for v in &viewers {
        v.spawn().await?;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut viewer_addrs = Vec::new();
    for v in &viewers {
        let addr = v.local_addr().await?;
        broadcaster.connect(addr).await?;
        viewer_addrs.push(addr);
    }
    for addr in &viewer_addrs {
        for _ in 0..50 {
            if broadcaster.connected_peers().contains(addr) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // Subscribe every viewer before broadcasting so none miss a packet
    // (the broadcast channel buffers up to 1024 frames).
    let mut rxs: Vec<_> = viewers.iter().map(Node::subscribe).collect();

    // The broadcaster injects a few real messages into the stream via
    // the high-priority lane; the muxer stamps them inline.
    let speaker = broadcaster.clone();
    tokio::spawn(async move {
        for line in [
            &b"EMERGENCY BROADCAST: meet at dawn"[..],
            &b"drop point is unchanged"[..],
            &b"burn this after reading"[..],
        ] {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let frame = build_ts_packet(Some(line));
            let _ = speaker
                .shaper()
                .enqueue_raw(Lane::High, Target::Broadcast, frame);
        }
    });

    // Let the stream run, buffering at each viewer, then drain.
    tokio::time::sleep(Duration::from_millis(2200)).await;

    println!("viewer-0's reassembled MPEG-TS stream (first packets):\n");
    println!(
        "  {:<5}  {:<6}  {:<5}  {:<6}  verdict",
        "pid", "scram", "cc", "pusi"
    );
    println!("  {}", "-".repeat(48));

    let start = Instant::now();
    let mut per_viewer = Vec::new();
    for (vi, rx) in rxs.iter_mut().enumerate() {
        let mut packets = 0usize;
        let mut valid = 0usize;
        let mut discontinuities = 0usize;
        let mut pids = HashSet::new();
        let mut last_cc: Option<u8> = None;
        let mut decoded: Vec<String> = Vec::new();
        let mut shown = 0usize;

        loop {
            match rx.try_recv() {
                Ok(buf) => {
                    packets += 1;
                    if let Some((pid, cc, pusi, scrambling)) = parse_ts(&buf) {
                        valid += 1;
                        pids.insert(pid);
                        if let Some(prev) = last_cc {
                            if cc != (prev + 1) & 0x0f {
                                discontinuities += 1;
                            }
                        }
                        last_cc = Some(cc);
                        let real = recover(&buf);
                        if let Some(ref m) = real {
                            decoded.push(String::from_utf8_lossy(m).into_owned());
                        }
                        if vi == 0 && shown < 10 {
                            println!(
                                "  0x{pid:03x}  {:<6}  {cc:<5}  {:<6}  TS ✓{}",
                                if scrambling != 0 { "scram" } else { "clear" },
                                if pusi { "start" } else { "" },
                                if real.is_some() { "  ← real" } else { "" },
                            );
                            shown += 1;
                        }
                    }
                }
                Err(TryRecvError::Empty | TryRecvError::Closed) => break,
                Err(TryRecvError::Lagged(_)) => continue,
            }
        }
        per_viewer.push((packets, valid, discontinuities, pids.len(), decoded));
    }
    let secs = start.elapsed().as_secs_f64().max(2.2);

    // --- summary ------------------------------------------------------------
    println!("\n=== what each viewer reassembled ===");
    for (i, (packets, valid, disc, n_pids, decoded)) in per_viewer.iter().enumerate() {
        println!(
            "  viewer-{i}: {packets} packets, {valid} valid TS, {disc} CC discontinuities, \
             {n_pids} PID, {} real msgs",
            decoded.len()
        );
    }
    let total: usize = per_viewer.iter().map(|v| v.0).sum();
    println!(
        "\n  aggregate bitrate     : {:.1} kbps",
        (total * TS_PACKET * 8) as f64 / secs / 1000.0
    );
    if let Some((_, _, _, _, decoded)) = per_viewer.first() {
        println!("  messages every viewer decoded : {decoded:?}");
    }

    println!("\nTo a DPI box this is one PID of constant-bitrate scrambled MPEG-TS");
    println!("fanned to {VIEWERS} viewers — a textbook IPTV multicast. Each viewer");
    println!("sees a gap-free continuity counter; the secret messages were muxed");
    println!("into the scrambled payload, indistinguishable from the noise.");

    println!("\n--- Notes ---");
    println!("* Global scope with fanout ≥ viewers is the multicast model: one");
    println!("  packet per tick to every viewer, so each sees a gap-free CC. A");
    println!("  smaller fanout (gossip) would deliver a random subset per tick and");
    println!("  show CC discontinuities — realistic as TS 'packet loss', but not a");
    println!("  clean single-source stream.");
    println!("* The continuity counter is shared across viewers (one stream); the");
    println!("  muxer's cover + finalize_real hooks both advance it, so real lane");
    println!("  frames and comfort packets interleave under one coherent counter.");

    broadcaster.shutdown().await;
    for v in &viewers {
        v.shutdown().await;
    }
    Ok(())
}
