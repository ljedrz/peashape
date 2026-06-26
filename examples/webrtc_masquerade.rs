//! Masquerading `peashape` traffic as a WebRTC media stream carried
//! over **TCP** — the TURN-TCP / RFC 4571 fallback browsers use when
//! UDP is blocked (corporate firewalls, restrictive NATs, captive
//! portals). This is a real, common shape on the wire, and — unlike
//! SRTP-over-UDP — it matches `peashape`'s actual transport: `pea2pea`
//! speaks length-delimited frames over TCP.
//!
//! # What a passive observer sees
//!
//! RFC 4571 ("Framing RTP over Connection-Oriented Transport") wraps
//! each RTP packet in a length prefix and streams them over TCP. That
//! is exactly what `peashape` already does: its codec writes a 4-byte
//! big-endian length followed by the frame. So a peashape stream whose
//! frames are RTP packets *is* an RFC-4571-style RTP-over-TCP stream:
//!
//! ```text
//!   [00 00 00 66] [ RTP packet, 0x66 = 102 bytes ]  ← repeated
//!    └─ length ─┘  └─ 12-byte RTP header + payload + SRTP tag ─┘
//! ```
//!
//! Each RTP packet carries the textbook cleartext header — fixed SSRC,
//! a 16-bit sequence number incrementing by one, a timestamp advancing
//! by 960 (= 48 kHz × 20 ms) — and an encrypted, random-looking
//! payload. One packet every 20 ms ⇒ 50 packets/second ⇒ a ~41 kbps
//! Opus call.
//!
//! # Real traffic rides the priority lanes; the generator sequences it
//!
//! Real messages go out the normal way — through `peashape`'s
//! high-priority lane — so they get the lane's priority and capacity
//! semantics. The trick that keeps the wire coherent is the custom
//! [`CoverGenerator`]'s two hooks, which share *one* RTP clock:
//!
//! - [`cover`](CoverGenerator::cover) stamps comfort-noise frames on
//!   empty ticks, and
//! - [`finalize_real`](CoverGenerator::finalize_real) stamps each real
//!   frame as the scheduler drains it from the lane, in send order.
//!
//! Because a single sequencer assigns every packet's header just
//! before it hits the wire, the sequence numbers a TCP observer
//! reassembles are perfectly monotonic and in order — whether the
//! packet carried comfort noise or a real message. The payload is
//! encrypted under a per-packet random nonce carried in the packet
//! (not under the sequence number), so re-stamping the header at
//! egress never disturbs decryption.
//!
//! Run with: cargo run --example webrtc_masquerade

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use rand::Rng;

use peashape::{CoverGenerator, Lane, Node, ShapeConfig, ShapingScope, ShapingStrategy, Target};

// --- WebRTC / RTP wire profile ---------------------------------------------

/// Opus dynamic payload type, as negotiated by virtually every
/// browser's SDP (`a=rtpmap:111 opus/48000/2`).
const OPUS_PT: u8 = 111;
/// RTP version 2, no padding/extension/CSRC — the first header byte
/// of essentially every WebRTC packet.
const RTP_V2: u8 = 0x80;
/// Bytes in a minimal RTP header.
const RTP_HEADER: usize = 12;
/// Bytes in the SRTP authentication tag appended to every packet
/// (HMAC-SHA1-80 → 10 bytes; the default WebRTC profile).
const SRTP_TAG: usize = 10;
/// Opus payload bytes per packet at a ~32 kbps CBR setting.
const OPUS_PAYLOAD: usize = 80;
/// Total on-the-wire packet size: 12 + 80 + 10 = 102 bytes.
const FRAME_SIZE: usize = RTP_HEADER + OPUS_PAYLOAD + SRTP_TAG;
/// Opus frame duration; one packet per this interval ⇒ 50 packets/s.
const PTIME: Duration = Duration::from_millis(20);
/// RTP timestamp increment per packet: 48 kHz clock × 20 ms = 960.
const TS_STEP: u32 = 960;
/// Shared secret used by the toy keystream (see `keystream`).
const KEY: u32 = 0x5a5a_5a5a;
/// Bytes of random per-packet nonce at the front of the payload.
const NONCE: usize = 4;

/// A 4-byte marker placed at the start of the *plaintext* of a real
/// frame so the receiver can tell real audio from comfort noise after
/// decrypting. Comfort/cover frames are pure random and won't match.
const REAL_MAGIC: &[u8; 4] = b"RTP!";

/// A toy keystream standing in for SRTP's AEAD. It is keyed on a
/// per-packet random `nonce` carried in the payload — **not** on the
/// sequence number — so the header can be (re)stamped at egress
/// without disturbing decryption, and identical plaintext never
/// produces identical ciphertext. This is **not** real crypto; in
/// production the DTLS-SRTP keys negotiated by a `pea2pea` `Handshake`
/// do this job.
fn keystream(nonce: u32, i: usize) -> u8 {
    let n = nonce.wrapping_add(i as u32).wrapping_mul(2_654_435_761);
    (KEY ^ n ^ (n >> 13)) as u8
}

/// Overwrites the 12-byte RTP header in place with a freshly-sequenced
/// one. This is the only thing the egress sequencer touches.
fn stamp_header(f: &mut [u8], seq: u16, ts: u32, ssrc: u32, marker: bool) {
    f[0] = RTP_V2;
    f[1] = if marker { 0x80 | OPUS_PT } else { OPUS_PT };
    f[2..4].copy_from_slice(&seq.to_be_bytes());
    f[4..8].copy_from_slice(&ts.to_be_bytes());
    f[8..12].copy_from_slice(&ssrc.to_be_bytes());
}

/// Builds one RTP packet of exactly `FRAME_SIZE` bytes with a *zeroed*
/// header (the sequencer stamps it later): `header || nonce ||
/// encrypted body || SRTP tag`. If `message` is `Some`, the packet
/// carries real ("encrypted") application data; if `None`, it is
/// comfort noise (random payload).
fn build_packet(message: Option<&[u8]>) -> BytesMut {
    let mut f = BytesMut::with_capacity(FRAME_SIZE);
    let mut rng = rand::rng();

    f.put_bytes(0, RTP_HEADER); // placeholder header, stamped at egress

    // payload region: random by default (== comfort noise, and a random
    // nonce for real frames).
    let mut payload = vec![0u8; OPUS_PAYLOAD];
    rng.fill(&mut payload[..]);
    if let Some(msg) = message {
        let nonce = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
        let take = msg.len().min(OPUS_PAYLOAD - NONCE - REAL_MAGIC.len() - 1);
        let mut pt = Vec::with_capacity(REAL_MAGIC.len() + 1 + take);
        pt.extend_from_slice(REAL_MAGIC);
        pt.push(take as u8);
        pt.extend_from_slice(&msg[..take]);
        for (i, b) in pt.iter().enumerate() {
            payload[NONCE + i] = b ^ keystream(nonce, i);
        }
    }
    f.extend_from_slice(&payload);

    let mut tag = [0u8; SRTP_TAG];
    rng.fill(&mut tag);
    f.extend_from_slice(&tag);

    debug_assert_eq!(f.len(), FRAME_SIZE);
    f
}

/// Parses the cleartext RTP header. Returns `None` for a frame that
/// isn't shaped like RTP. Returns `(seq, ts, ssrc, marker)` for a
/// well-formed Opus packet.
fn parse_rtp(frame: &[u8]) -> Option<(u16, u32, u32, bool)> {
    if frame.len() < RTP_HEADER || frame[0] != RTP_V2 || frame[1] & 0x7f != OPUS_PT {
        return None;
    }
    let marker = frame[1] & 0x80 != 0;
    let seq = u16::from_be_bytes([frame[2], frame[3]]);
    let ts = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let ssrc = u32::from_be_bytes([frame[8], frame[9], frame[10], frame[11]]);
    Some((seq, ts, ssrc, marker))
}

/// Recovers a real application message from a received frame, or
/// `None` if it is comfort noise. Reads the per-packet nonce from the
/// payload to reconstruct the keystream.
fn recover(frame: &[u8]) -> Option<Vec<u8>> {
    let payload = frame.get(RTP_HEADER..RTP_HEADER + OPUS_PAYLOAD)?;
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

// --- the cover generator: one RTP clock for the whole stream ---------------

/// Owns one RTP stream's header state. As a [`CoverGenerator`] it
/// stamps *every* outgoing packet — comfort noise via
/// [`cover`](CoverGenerator::cover) and real lane frames via
/// [`finalize_real`](CoverGenerator::finalize_real) — from a single,
/// shared sequence-number / timestamp clock, so the stream a TCP peer
/// reassembles is perfectly in order regardless of which slots carried
/// real audio.
struct RtpSequencer {
    ssrc: u32,
    seq: AtomicU16,
    ts: AtomicU32,
    started: AtomicBool,
}

impl RtpSequencer {
    fn new() -> Self {
        let mut rng = rand::rng();
        Self {
            ssrc: rng.random(),
            seq: AtomicU16::new(rng.random()),
            ts: AtomicU32::new(rng.random()),
            started: AtomicBool::new(false),
        }
    }

    /// Stamps the next sequenced RTP header onto `frame` in place.
    fn stamp(&self, frame: &mut [u8]) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let ts = self.ts.fetch_add(TS_STEP, Ordering::Relaxed);
        // The RTP marker bit flags the first packet of a talkspurt.
        let marker = !self.started.swap(true, Ordering::Relaxed);
        stamp_header(frame, seq, ts, self.ssrc, marker);
    }
}

impl CoverGenerator for RtpSequencer {
    fn cover(&self, _config: &ShapeConfig) -> BytesMut {
        let mut f = build_packet(None);
        self.stamp(&mut f);
        f
    }

    fn finalize_real(&self, _config: &ShapeConfig, mut frame: BytesMut) -> BytesMut {
        self.stamp(&mut frame);
        frame
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("peashape — masquerading as WebRTC media over TCP (RFC 4571 / TURN-TCP)\n");
    println!("profile: PT={OPUS_PT} (opus/48000/2), {FRAME_SIZE}-byte RTP packets over TCP,");
    println!(
        "         one every {} ms ({} pps), ~{} kbps + a 4-byte length prefix per packet\n",
        PTIME.as_millis(),
        1000 / PTIME.as_millis(),
        FRAME_SIZE * 8 * (1000 / PTIME.as_millis() as usize) / 1000,
    );

    // The caller's outbound link is shaped as a WebRTC-over-TCP call.
    // Its cover generator owns the RTP sequencing for both cover and
    // real (lane) frames.
    let rtp = Arc::new(RtpSequencer::new());
    let cover: Arc<dyn CoverGenerator> = rtp.clone();
    let caller = Node::new(ShapeConfig {
        name: Some("caller".into()),
        listener_addr: Some("127.0.0.1:0".parse()?),
        strategy: ShapingStrategy::Constant { interval: PTIME },
        scope: ShapingScope::PerConnection { randomize: false },
        frame_size: FRAME_SIZE,
        cover_generator: Some(cover),
        ..Default::default()
    });
    // The callee is the other end of the call and our observation point.
    let callee = Node::new(ShapeConfig {
        name: Some("callee".into()),
        listener_addr: Some("127.0.0.1:0".parse()?),
        strategy: ShapingStrategy::Constant {
            interval: Duration::from_secs(10),
        },
        scope: ShapingScope::PerConnection { randomize: false },
        frame_size: FRAME_SIZE,
        ..Default::default()
    });

    caller.spawn().await?;
    callee.spawn().await?;
    let callee_addr = callee.local_addr().await?;
    caller.connect(callee_addr).await?;
    for _ in 0..50 {
        if caller.connected_peers().contains(&callee_addr) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut rx = callee.subscribe();

    // The "speaker": hands a few real messages to the node's
    // high-priority lane during the call. They preempt comfort noise on
    // the next tick, and the sequencer stamps them in order. Everything
    // else on the wire is comfort noise — and a passive observer cannot
    // tell which is which.
    let speaker = caller.clone();
    tokio::spawn(async move {
        for line in [
            &b"hi bob"[..],
            &b"meet at the usual place"[..],
            &b"bring the keys"[..],
        ] {
            tokio::time::sleep(Duration::from_millis(500)).await;
            // A real frame: built with a zeroed header (the sequencer
            // stamps it at egress) and submitted to the high lane.
            let frame = build_packet(Some(line));
            let _ = speaker
                .shaper()
                .enqueue_raw(Lane::High, Target::Unicast(callee_addr), frame);
        }
    });

    // --- the observer (what a passive TCP wire-tap reassembles) -------------
    println!("reassembled RTP-over-TCP stream (first packets):\n");
    println!(
        "  {:<6}  {:<10}  {:<10}  {:<6}  verdict",
        "seq", "ts", "ssrc", "mark"
    );
    println!("  {}", "-".repeat(56));

    let mut total = 0usize;
    let mut valid = 0usize;
    let mut cover_count = 0usize;
    let mut gaps = 0usize;
    let mut ssrcs = std::collections::HashSet::new();
    let mut last_seq: Option<u16> = None;
    let mut recovered: Vec<String> = Vec::new();
    let mut shown = 0usize;

    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(2300) {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await {
            total += 1;
            match parse_rtp(&buf) {
                Some((seq, ts, ssrc, marker)) => {
                    valid += 1;
                    ssrcs.insert(ssrc);
                    if let Some(prev) = last_seq {
                        gaps += (seq.wrapping_sub(prev) as usize).saturating_sub(1);
                    }
                    last_seq = Some(seq);
                    let is_real = recover(&buf).inspect(|m| {
                        recovered.push(String::from_utf8_lossy(m).into_owned());
                    });
                    if shown < 12 {
                        println!(
                            "  {seq:<6}  {ts:<10}  {ssrc:08x}    {:<6}  RTP ✓{}",
                            if marker { "M" } else { "" },
                            if is_real.is_some() { "  ← real" } else { "" },
                        );
                        shown += 1;
                    }
                }
                None => {
                    cover_count += 1;
                    if shown < 12 {
                        println!(
                            "  {:<6}  {:<10}  {:<10}  {:<6}  ✗ non-RTP",
                            "?", "?", "?", ""
                        );
                        shown += 1;
                    }
                }
            }
        }
    }

    // --- summary ------------------------------------------------------------
    let secs = start.elapsed().as_secs_f64();
    println!("\n=== observed stream profile ===");
    println!("  packets               : {total}");
    println!("  rate                  : {:.1} pps", total as f64 / secs);
    println!(
        "  bitrate               : {:.1} kbps",
        (total * FRAME_SIZE * 8) as f64 / secs / 1000.0
    );
    println!(
        "  parsed as valid RTP   : {valid}/{total} ({:.1}%)",
        100.0 * valid as f64 / total.max(1) as f64
    );
    println!("  non-RTP frames        : {cover_count}");
    println!(
        "  distinct SSRCs        : {} (a real call has exactly 1)",
        ssrcs.len()
    );
    println!("  sequence-number gaps  : {gaps} (TCP is in-order, so a real call has 0)");
    println!("  real messages decoded : {recovered:?}");

    println!("\nTo a DPI box this is a textbook 50 pps Opus call tunneled over TCP:");
    println!("one SSRC, gap-free monotonic seq/ts, a 4-byte-length-prefixed record");
    println!("per packet. The real messages rode the priority lane, were sequenced");
    println!("inline with the comfort noise, and are indistinguishable on the wire.");

    println!("\n--- Notes ---");
    println!("* Real traffic flows through peashape's high-priority lane; the cover");
    println!("  generator's finalize_real hook stamps each lane frame with the next");
    println!("  RTP sequence number as it leaves, so a single clock drives the whole");
    println!("  stream and a TCP observer sees no gaps or reordering.");
    println!("* RFC 4571 uses a 2-byte length prefix; peashape's codec uses 4 bytes.");
    println!("  For frames < 64 KiB the high two bytes are zero, so the framing is");
    println!("  RFC-4571-shaped with an extra 00 00 per packet.");

    caller.shutdown().await;
    callee.shutdown().await;
    Ok(())
}
