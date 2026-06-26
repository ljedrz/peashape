//! Two nodes exchanging a single real application message
//! through `peashape`'s priority lane. Demonstrates the
//! minimal end-to-end usage pattern: Alice shapes and sends a
//! real message, Bob receives it over the cover stream and
//! extracts the payload.
//!
//! This is the template you'd adapt to build an app on
//! `peashape`: subscribe, filter incoming frames by a
//! recognizable payload format, and handle the ones that are
//! real.
//!
//! PRIVACY NOTE: for clarity this example marks real frames
//! with a *plaintext* magic header, which is visible on the
//! wire — a passive observer could use it to tell real frames
//! from cover, defeating the metadata-privacy property. It is
//! fine for understanding the mechanics, but a real deployment
//! must make the payload indistinguishable from random by
//! encrypting it (authenticated encryption: a frame that fails
//! to authenticate is cover). The `webrtc_masquerade` and
//! `mpegts_broadcast` examples show payloads that look random
//! on the wire.
//!
//! Run with: cargo run --example two_nodes

use std::time::Duration;

use peashape::{ID_SIZE, Node, ShapeConfig, ShapingStrategy};

/// A minimal application-level payload format. Real messages
/// start with this 4-byte magic header followed by a 1-byte
/// length and the application data; random cover bytes match
/// the magic with probability 1/2^32, which is negligible over
/// any realistic observation window. (Use a longer magic, a
/// MAC, or a version byte + structured prefix if your threat
/// model cares about collision probability or forgery.)
///
/// The length prefix is necessary because `peashape` pads
/// every real message with random bytes to the fixed
/// `frame_size`, so the receiver cannot tell where the
/// application data ends without it.
const MAGIC: &[u8; 4] = b"PESH";

/// Frames a real application payload for transmission over
/// `peashape`: `MAGIC || len || data`, where `len` is a single
/// byte. Panics if `data` is longer than 255 bytes (raise
/// `frame_size` or use a wider length field if you need more).
fn frame_payload(data: &[u8]) -> Vec<u8> {
    assert!(data.len() <= 255, "payload too long for 1-byte length");
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + data.len());
    out.extend_from_slice(MAGIC);
    out.push(data.len() as u8);
    out.extend_from_slice(data);
    out
}

/// Returns the application payload if `frame` is a real message
/// (i.e. it starts with the magic header and a valid length),
/// or `None` if it is cover traffic. The random padding
/// `peashape` appends is stripped using the embedded length.
fn extract_payload(frame: &[u8]) -> Option<&[u8]> {
    // The first 32 bytes are the random message ID (see
    // `peashape::ID_SIZE`); the application payload starts
    // after it.
    let payload = frame.get(ID_SIZE..)?;
    let rest = payload.strip_prefix(MAGIC)?;
    let len = *rest.first()? as usize;
    rest.get(1..1 + len)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mk_node = |name: &str| {
        Ok::<_, Box<dyn std::error::Error>>(Node::new(ShapeConfig {
            name: Some(name.into()),
            listener_addr: Some("127.0.0.1:0".parse()?),
            strategy: ShapingStrategy::Constant {
                interval: Duration::from_millis(100),
            },
            ..Default::default()
        }))
    };

    let alice = mk_node("alice")?;
    let bob = mk_node("bob")?;
    alice.spawn().await?;
    bob.spawn().await?;

    // Wire them up. `connect` is directional; for traffic to
    // flow both ways you'd typically connect both directions,
    // but for this demo Alice broadcasting to Bob is enough.
    alice.connect(bob.local_addr().await?).await?;
    // Wait for the connection to be established.
    let bob_addr = bob.local_addr().await?;
    for _ in 0..50 {
        if alice.connected_peers().contains(&bob_addr) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Bob subscribes *before* Alice broadcasts, so he doesn't
    // miss the frame. The broadcast channel is bounded (1024);
    // late subscribers miss earlier messages.
    let mut bob_rx = bob.subscribe();

    // Alice broadcasts a real message. The payload is framed
    // with the magic header + length prefix so Bob can pick it
    // out of the cover stream and strip the random padding.
    let real_payload = frame_payload(b"hello, peashape!");
    let pub_id = alice.broadcast_shaped(&real_payload)?;
    println!("Alice broadcast message with id {pub_id:?}");

    // Bob drains frames until he sees the real one. Most
    // frames will be cover (random bytes) and are skipped by
    // `extract_payload`.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let mut got = None;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), bob_rx.recv()).await {
            Ok(Ok(frame)) => {
                if let Some(payload) = extract_payload(&frame) {
                    got = Some(payload.to_vec());
                    break;
                }
            }
            _ => continue,
        }
    }

    match got {
        Some(payload) => {
            println!(
                "Bob received the real message: {:?}",
                String::from_utf8_lossy(&payload)
            );
        }
        None => {
            println!("Bob did not receive the real message within the deadline.");
        }
    }

    alice.shutdown().await;
    bob.shutdown().await;
    Ok(())
}
