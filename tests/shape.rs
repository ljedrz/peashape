//! Integration tests for `peashape`.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use peashape::{
    connect_nodes, Lane, Node, ShapeConfig, ShapingScope, ShapingStrategy, Topology, ID_SIZE,
};

/// Returns a [`ShapeConfig`] tailored for tests: small messages,
/// fast shaping schedule, loopback listener, and a permissive
/// per-IP connection cap.
fn test_config(name: &str, strategy: ShapingStrategy, scope: ShapingScope) -> ShapeConfig {
    ShapeConfig {
        name: Some(name.into()),
        listener_addr: Some("127.0.0.1:0".parse().unwrap()),
        strategy,
        scope,
        fanout: 3,
        frame_size: 128,
        high_lane_capacity: 16,
        low_lane_capacity: 4,
        max_connections: 32,
        max_connections_per_ip: 8,
        ..Default::default()
    }
}

/// Returns true if the buffer `haystack` contains the needle
/// `payload`. Because the shaped frame is padded with random
/// bytes, we look for the marker as a substring.
fn contains_payload(haystack: &[u8], payload: &[u8]) -> bool {
    if payload.is_empty() {
        return true;
    }
    haystack.windows(payload.len()).any(|w| w == payload)
}

/// Spins until `addr` is in `node.connected_peers()`, with a
/// 500 ms timeout. Returns `true` if the connection was
/// observed.
async fn wait_connected(node: &Node, addr: std::net::SocketAddr) -> bool {
    for _ in 0..50 {
        if node.connected_peers().contains(&addr) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unicast_real_message_arrives() {
    // Two nodes, a 50 ms cover schedule, fanout 1. Alice
    // publishes a single real message; Bob receives it.
    let alice = Node::new(test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    let bob = Node::new(test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(
        wait_connected(&alice, bob_addr).await,
        "connection never established"
    );

    let mut bob_rx = bob.subscribe();
    let marker = b"peashape-direct-marker".to_vec();
    let pub_id = alice.broadcast_shaped(&marker).expect("broadcast");

    // Wait for the marker to arrive at bob.
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut got = false;
    while !got && Instant::now() < deadline {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), bob_rx.recv()).await {
            if contains_payload(&buf, &marker) {
                got = true;
            }
        }
    }
    assert!(
        got,
        "bob never saw the marker broadcast by alice (id {:?})",
        pub_id
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unicast_to_one_peer() {
    // Three nodes in a mesh. Alice sends a real message to *one*
    // specific peer (Bob), with a 50 ms schedule. Bob should
    // receive it; Carol should not (with high probability
    // within the 1 s deadline given the random cover traffic).
    let mut nodes = Vec::new();
    for name in &["alice", "bob", "carol"] {
        let node = Node::new(test_config(
            name,
            ShapingStrategy::Constant {
                interval: Duration::from_millis(50),
            },
            ShapingScope::Global,
        ));
        node.spawn().await.unwrap();
        nodes.push(node);
    }
    tokio::time::sleep(Duration::from_millis(20)).await;

    connect_nodes(&nodes, Topology::Mesh)
        .await
        .expect("connect");

    let mut bobs = [nodes[1].subscribe(), nodes[2].subscribe()];
    let bob_addr = nodes[1].local_addr().await.unwrap();
    let carol_addr = nodes[2].local_addr().await.unwrap();

    let marker = b"peashape-unicast-marker".to_vec();
    let _id = nodes[0]
        .send_shaped(bob_addr, &marker)
        .expect("send_shaped");
    // We deliberately also send to carol so the test is
    // meaningful (alice is *not* broadcasting, so any frame
    // carol sees must be a cover frame — which won't match the
    // marker).
    let _id2 = nodes[0]
        .send_shaped(carol_addr, b"different payload for carol")
        .expect("send_shaped");

    let deadline = Instant::now() + Duration::from_secs(1);
    let mut bob_got = false;
    let mut carol_got = false;
    while !(bob_got && carol_got) && Instant::now() < deadline {
        for (i, rx) in bobs.iter_mut().enumerate() {
            if (i == 0 && bob_got) || (i == 1 && carol_got) {
                continue;
            }
            if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
                if contains_payload(&buf, &marker) {
                    if i == 0 {
                        bob_got = true;
                    } else {
                        carol_got = true;
                    }
                }
            }
        }
    }

    assert!(bob_got, "bob never received the unicast marker");
    assert!(!carol_got, "carol (a non-target) saw the unicast marker");

    for n in &nodes {
        n.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unicast_to_disconnected_peer_is_dropped_silently() {
    // If the user calls `send_shaped(peer, ...)` and the peer
    // disconnects between submission and the tick that drains
    // the lane, the frame is silently dropped (with a debug
    // log). The node must not panic and must continue to serve
    // cover traffic.
    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    );
    // A very slow shaping rate so the lane stays full long
    // enough for bob to disconnect.
    alice_config.strategy = ShapingStrategy::Constant {
        interval: Duration::from_secs(60),
    };
    let alice = Node::new(alice_config);

    let mut bob_config = test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_secs(10),
        },
        ShapingScope::Global,
    );
    bob_config.frame_size = alice.config().frame_size;
    let bob = Node::new(bob_config);

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    // Submit a unicast; then disconnect bob *before* the next
    // tick. The lane is FIFO with 1s/s no-tick rate, so the
    // frame will still be there at the disconnect time.
    alice
        .send_shaped(bob_addr, b"this should be dropped")
        .expect("send_shaped");
    alice.disconnect(bob_addr).await;

    // After a short wait, no panic should have occurred; the
    // node should still be operational. We don't have a
    // straightforward way to verify the frame was dropped, but
    // the absence of a panic is the main correctness check.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        alice.connected_peers().len(),
        0,
        "bob should be disconnected"
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn constant_rate_traffic() {
    // Two connected nodes; with a 50 ms cover interval we
    // expect ~20 messages / second in each direction over a 1 s
    // measurement window. Allow generous slack.
    let alice = Node::new(test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    let bob = Node::new(test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    let mut rx = bob.subscribe();
    let start = Instant::now();
    let mut count = 0usize;
    while start.elapsed() < Duration::from_secs(1) {
        if tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_ok()
        {
            count += 1;
        }
    }

    assert!(
        (8..=60).contains(&count),
        "expected ~20 shaped messages per second, got {count}",
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn poisson_rate_traffic() {
    // Same shape as the constant-rate test, but with a Poisson
    // schedule at 25 msg/s on average. Allow wide slack because
    // the 1 s window is short relative to the variance.
    let alice = Node::new(test_config(
        "alice",
        ShapingStrategy::Poisson { rate: 25.0 },
        ShapingScope::Global,
    ));
    let bob = Node::new(test_config(
        "bob",
        ShapingStrategy::Poisson { rate: 25.0 },
        ShapingScope::Global,
    ));

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    let mut rx = bob.subscribe();
    let start = Instant::now();
    let mut count = 0usize;
    while start.elapsed() < Duration::from_secs(2) {
        if tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_ok()
        {
            count += 1;
        }
    }

    assert!(
        (10..=200).contains(&count),
        "expected ~50 shaped messages over 2 s at rate 25/s, got {count}",
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cover_only_when_lanes_are_empty() {
    // A node that never has real traffic still emits the
    // configured cover rate. This is the central property of
    // the metadata-privacy claim: traffic is *always* flowing
    // at the configured rate.
    let alice = Node::new(test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    let bob = Node::new(test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_secs(10),
        },
        ShapingScope::Global,
    ));

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    // No `broadcast_shaped` calls — only cover traffic flows.
    let mut rx = bob.subscribe();
    let start = Instant::now();
    let mut count = 0usize;
    while start.elapsed() < Duration::from_secs(1) {
        if tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_ok()
        {
            count += 1;
        }
    }
    assert!(
        (8..=60).contains(&count),
        "expected ~20 cover frames per second (no real traffic), got {count}",
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn high_priority_drained_before_low_priority() {
    // Submit one high-priority and one low-priority message;
    // observe that the high-priority message arrives at the
    // peer *before* the low-priority one, even though the low
    // one was submitted first.
    let alice = Node::new(test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    let bob = Node::new(test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    let mut bob_rx = bob.subscribe();
    let high_marker = b"peashape-HIGH-priority-marker";
    let low_marker = b"peashape-LOW-priority-marker";

    // Submit the low one first.
    alice.broadcast_shaped_low(low_marker).expect("low enqueue");
    alice.broadcast_shaped(high_marker).expect("high enqueue");

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut high_seen_at: Option<Instant> = None;
    let mut low_seen_at: Option<Instant> = None;
    let window_start = Instant::now();
    while (high_seen_at.is_none() || low_seen_at.is_none()) && Instant::now() < deadline {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), bob_rx.recv()).await {
            let now = Instant::now();
            if high_seen_at.is_none() && contains_payload(&buf, high_marker) {
                high_seen_at = Some(now);
            }
            if low_seen_at.is_none() && contains_payload(&buf, low_marker) {
                low_seen_at = Some(now);
            }
        }
    }
    let (Some(h), Some(l)) = (high_seen_at, low_seen_at) else {
        panic!("did not observe both messages within the deadline");
    };
    assert!(
        h <= l,
        "high-priority message arrived at {:?} (relative to window start {:?}) \
         after low-priority message at {:?}",
        h - window_start,
        Duration::ZERO,
        l - window_start,
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn payload_too_large_is_rejected() {
    let node = Node::new(test_config(
        "node",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    node.spawn().await.unwrap();

    // 128-byte frame; ID_SIZE is 32; so 96 bytes is the max
    // user payload. 97 bytes should be rejected.
    let too_big = vec![0u8; 97];
    let err = node.broadcast_shaped(&too_big).unwrap_err();
    assert!(err.to_string().contains("payload too large"));

    // Same check for the low lane.
    let err = node.broadcast_shaped_low(&too_big).unwrap_err();
    assert!(err.to_string().contains("payload too large"));

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lane_full_is_rejected() {
    // Set the high-priority capacity to 1, queue a message, then
    // try to enqueue a second; the second should be rejected.
    let mut config = test_config(
        "node",
        ShapingStrategy::Constant {
            interval: Duration::from_secs(60),
        },
        ShapingScope::Global,
    );
    config.high_lane_capacity = 1;
    let node = Node::new(config);
    node.spawn().await.unwrap();

    node.broadcast_shaped(b"first").expect("first enqueue");
    let err = node.broadcast_shaped(b"second").unwrap_err();
    assert!(err.to_string().contains("priority lane is full"));

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn low_lane_silently_evicts_oldest() {
    // The low-priority lane is LIFO with drop-oldest: even when
    // saturated, enqueueing always succeeds (the oldest is
    // evicted).
    let mut config = test_config(
        "node",
        ShapingStrategy::Constant {
            interval: Duration::from_secs(60),
        },
        ShapingScope::Global,
    );
    config.low_lane_capacity = 2;
    let node = Node::new(config);
    node.spawn().await.unwrap();

    for i in 0..5 {
        let payload = format!("msg-{i}");
        node.broadcast_shaped_low(payload.as_bytes())
            .expect("enqueue must succeed even when full");
    }

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_connection_scope_sends_to_one_peer_per_tick() {
    // PerConnection scope: each tick sends to exactly one
    // peer. With one connected peer and a 50 ms interval, we
    // expect ~20 frames/second in the 1 s window.
    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::PerConnection { randomize: false },
    );
    alice_config.fanout = 1; // explicit: per-connection ignores fanout
    let alice = Node::new(alice_config);

    let mut bob = test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_secs(10),
        },
        ShapingScope::Global,
    );
    bob.frame_size = alice.config().frame_size;
    let bob = Node::new(bob);

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    let mut rx = bob.subscribe();
    let start = Instant::now();
    let mut count = 0usize;
    while start.elapsed() < Duration::from_secs(1) {
        if tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .is_ok()
        {
            count += 1;
        }
    }
    assert!(
        (8..=60).contains(&count),
        "expected ~20 frames per second under PerConnection scope, got {count}",
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn frame_size_is_constant() {
    // Every frame observed on the wire should be exactly
    // `frame_size` bytes, regardless of payload size.
    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(20),
        },
        ShapingScope::Global,
    );
    alice_config.frame_size = 200;
    let alice = Node::new(alice_config);

    let mut bob_config = test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_secs(10),
        },
        ShapingScope::Global,
    );
    bob_config.frame_size = 200;
    let bob = Node::new(bob_config);

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    // Publish payloads of varying sizes; verify the wire-frame
    // size is always 200.
    alice.broadcast_shaped(&[0u8; 5]).unwrap();
    alice.broadcast_shaped(&[0u8; 50]).unwrap();
    alice.broadcast_shaped(&[0u8; 100]).unwrap();
    alice.broadcast_shaped(&[0u8; 167]).unwrap(); // 200 - 32 (ID) - 1

    let mut rx = bob.subscribe();
    let start = Instant::now();
    let mut sizes = Vec::new();
    while start.elapsed() < Duration::from_secs(1) {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            sizes.push(buf.len());
        }
    }
    assert!(!sizes.is_empty(), "no frames received");
    for s in &sizes {
        assert_eq!(*s, 200, "frame of size {s} on the wire (expected 200)");
    }

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_shaped_re_broadcasts_pre_built_frame() {
    // The `relay_shaped` API accepts a pre-built frame (of
    // exactly `frame_size` bytes) and ships it through the
    // low-priority lane. Useful for re-broadcasting frames
    // received from peers byte-for-byte unchanged.
    let alice = Node::new(test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    let bob = Node::new(test_config(
        "bob",
        ShapingStrategy::Constant {
            interval: Duration::from_secs(10),
        },
        ShapingScope::Global,
    ));
    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);

    // Build a frame of exactly `frame_size` bytes (128 in
    // test_config). The first 32 are a fake ID; the rest is
    // a recognizable payload.
    let mut frame = BytesMut::with_capacity(alice.config().frame_size);
    frame.extend_from_slice(&[0xAB; ID_SIZE]);
    let marker = b"peashape-relay-marker";
    frame.extend_from_slice(marker);
    let pad = alice.config().frame_size - ID_SIZE - marker.len();
    frame.extend_from_slice(&vec![0u8; pad]);

    alice.relay_shaped(frame.clone()).expect("relay_shaped");

    // Bob should see the relayed frame.
    let mut bob_rx = bob.subscribe();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut got = false;
    while !got && Instant::now() < deadline {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), bob_rx.recv()).await {
            if buf == frame {
                got = true;
            }
        }
    }
    assert!(got, "bob never received the relayed frame");

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relay_shaped_rejects_wrong_size() {
    // `relay_shaped` requires the frame to be exactly
    // `frame_size` bytes; anything else returns
    // `Error::FrameSizeMismatch`.
    let node = Node::new(test_config(
        "node",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    ));
    node.spawn().await.unwrap();

    let too_small = BytesMut::from(&[0u8; 10][..]);
    let err = node.relay_shaped(too_small).unwrap_err();
    assert!(err.to_string().contains("frame size mismatch"));

    let too_big = BytesMut::from(&vec![0u8; 256][..]);
    let err = node.relay_shaped(too_big).unwrap_err();
    assert!(err.to_string().contains("frame size mismatch"));

    node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lane_re_export_works() {
    // The Lane enum is part of the public API; verify it can be
    // matched on.
    let _ = Lane::High;
    let _ = Lane::Low;
    assert_ne!(Lane::High, Lane::Low);
}
