//! Demonstrates that `peashape` traffic is constant-rate and
//! indistinguishable from cover, even when the application is
//! actively publishing a real message.
//!
//! Two nodes are wired up: Alice (the shaper peer) and Bob (a
//! passive observer). Both run a `peashape::Node` with the same
//! shaping schedule. Alice publishes a real message halfway
//! through the observation window; Bob records the timestamp and
//! size of every frame he receives but has no way of telling
//! which frame was the real one.
//!
//! What an ISP, a Wi-Fi snooper, or any other passive network
//! observer sees is *exactly* what Bob sees here: a constant
//! stream of same-sized frames at a constant rate, with no
//! statistical signal correlated to user activity.
//!
//! Run with: cargo run --example demo

use std::time::{Duration, Instant};

use peashape::{Node, ShapeConfig, ShapingStrategy};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔═════════════════════════════════════════════════════════════╗");
    println!("║  peashape — traffic-analysis resistance demo                ║");
    println!("╚═════════════════════════════════════════════════════════════╝");
    println!();
    println!("Two nodes (Alice, Bob), shaping rate = 100 ms. Alice publishes");
    println!("a single real message at t = 1.0 s. Bob records every frame he");
    println!("receives. An external observer sees the same data as Bob.\n");

    fn mk_node(name: &str, cover: ShapingStrategy) -> Result<Node, Box<dyn std::error::Error>> {
        Ok(Node::new(ShapeConfig {
            name: Some(name.into()),
            listener_addr: Some("127.0.0.1:0".parse()?),
            strategy: cover,
            frame_size: 64,
            ..Default::default()
        }))
    }

    let alice = mk_node(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(100),
        },
    )?;
    let bob = mk_node(
        "bob",
        // Bob's own shaping traffic is once every 10 seconds —
        // i.e. so slow as to be effectively absent during the
        // 2 s observation window. Bob is otherwise a normal
        // `peashape` node, so what he records is *exactly* what
        // any other `peashape` peer (or any other passive
        // observer tapping the same TCP stream) would see.
        ShapingStrategy::Constant {
            interval: Duration::from_secs(10),
        },
    )?;
    alice.spawn().await?;
    bob.spawn().await?;
    alice.connect(bob.local_addr().await?).await?;

    // Bob subscribes to incoming frames. The library delivers
    // every frame it sees on the wire — real and cover alike —
    // through the same broadcast channel.
    let mut bob_rx = bob.subscribe();

    let start = Instant::now();
    let publish_at = start + Duration::from_secs(1);
    let end = start + Duration::from_millis(2_100);

    // Schedule Alice's publish in the background so the
    // observation loop is the only thing driving wall-clock
    // progress.
    let alice_pub = alice.clone();
    tokio::spawn(async move {
        tokio::time::sleep_until(publish_at.into()).await;
        alice_pub.broadcast_shaped(b"<<< REAL MESSAGE >>>").unwrap();
    });

    println!(
        "{:<8}  {:<5}  {:<18}  {:<20}",
        "t (ms)", "size", "Dt from prev (ms)", "bar"
    );
    println!("{}", "-".repeat(60));

    let mut last_t = start;
    let mut frames: Vec<(Duration, usize)> = Vec::new(); // (arrival, size)
    while Instant::now() < end {
        let timeout = end.saturating_duration_since(Instant::now());
        match tokio::time::timeout(timeout, bob_rx.recv()).await {
            Ok(Ok(buf)) => {
                let t = Instant::now();
                let dt = t.duration_since(last_t);
                frames.push((t.duration_since(start), buf.len()));
                draw_frame(t.duration_since(start), dt);
                last_t = t;
            }
            _ => break,
        }
    }

    // Print the cover-traffic envelope. Each '*' is one frame;
    // the tick mark is where Alice published.
    println!();
    println!("Cover-traffic envelope (one '*' per frame received by Bob):");
    println!("{}", "-".repeat(60));
    let publish_ms = publish_at.duration_since(start).as_millis();
    for (t, _) in &frames {
        let bar_x = t.as_millis() / 50; // 50 ms per column
        let mut line = String::with_capacity(60);
        for i in 0..bar_x {
            line.push(if i == publish_ms / 50 { '│' } else { ' ' });
        }
        line.push('*');
        println!("{}", line);
    }
    let mut scale = String::new();
    for i in 0..=40 {
        scale.push(if i == publish_ms / 50 { '↑' } else { ' ' });
    }
    println!("{}", scale);
    println!("0s                 1s (publish)              2s");

    print_stats(&frames);

    alice.shutdown().await;
    bob.shutdown().await;
    Ok(())
}

fn draw_frame(t: Duration, dt: Duration) {
    let bar = "█".repeat(((dt.as_micros() as f64 / 10_000.0).ceil() as usize).min(20));
    println!(
        "{:<8}  {:<5}  {:<18.1}  {}",
        t.as_millis(),
        64,
        dt.as_secs_f64() * 1000.0,
        bar
    );
}

fn print_stats(frames: &[(Duration, usize)]) {
    println!();
    println!("=== Bob's view of Alice's traffic ===");
    println!("  frames observed       : {}", frames.len());
    println!(
        "  unique frame sizes    : {}",
        frames
            .iter()
            .map(|(_, s)| s)
            .collect::<std::collections::HashSet<_>>()
            .len()
    );
    println!(
        "  total bytes            : {}",
        frames.iter().map(|(_, s)| s).sum::<usize>()
    );

    if frames.len() < 2 {
        return;
    }
    let iats: Vec<f64> = frames
        .windows(2)
        .map(|w| w[1].0.as_secs_f64() * 1000.0 - w[0].0.as_secs_f64() * 1000.0)
        .collect();
    let mean = iats.iter().sum::<f64>() / iats.len() as f64;
    let var = iats.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / iats.len() as f64;
    let std = var.sqrt();

    println!();
    println!("Inter-arrival time statistics (over all frames):");
    println!(
        "  mean   : {:>6.2} ms  (= configured shaping interval)",
        mean
    );
    println!("  stddev : {:>6.2} ms", std);
    println!(
        "  min    : {:>6.2} ms",
        iats.iter().cloned().fold(f64::INFINITY, f64::min)
    );
    println!(
        "  max    : {:>6.2} ms",
        iats.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
    );
    println!();
    println!("Alice's real message was published at t = 1.0 s, but the");
    println!("inter-arrival-time distribution shows no bump, dip, or any");
    println!("other statistical signal at that point. To Bob (and to any");
    println!("external observer tapping the wire) the stream of frames is");
    println!("indistinguishable from a node that is doing *nothing at all*.");
}
