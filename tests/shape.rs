//! Integration tests for `peashape`.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use peashape::{
    connect_nodes, CoverGenerator, Lane, Node, ShapeConfig, ShapingScope, ShapingStrategy,
    Topology, ID_SIZE,
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
async fn disconnected_unicast_does_not_leak_to_other_peers() {
    // Regression test: a unicast frame whose target has disconnected
    // between submission and tick time must be *dropped* — never
    // broadcast to other peers. Otherwise a payload addressed to one
    // peer would be delivered to peers it was never meant for. The
    // tick still emits `fanout` *cover* frames (preserving the rate),
    // so the only observable difference is that the real marker never
    // reaches a non-target.
    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(25),
        },
        ShapingScope::Global,
    );
    alice_config.fanout = 3;
    let alice = Node::new(alice_config);

    // Receivers shape very slowly so their own cover traffic does not
    // pollute the measurement.
    let mk = |name: &str| {
        let mut cfg = test_config(
            name,
            ShapingStrategy::Constant {
                interval: Duration::from_secs(10),
            },
            ShapingScope::Global,
        );
        cfg.frame_size = 128; // match alice (test_config default)
        Node::new(cfg)
    };
    let carol = mk("carol");
    let dave = mk("dave");
    // `ghost` is connected only long enough to learn its address, then
    // disconnected, so unicasts addressed to it have a gone target.
    let ghost = mk("ghost");

    alice.spawn().await.unwrap();
    carol.spawn().await.unwrap();
    dave.spawn().await.unwrap();
    ghost.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let carol_addr = carol.local_addr().await.unwrap();
    let dave_addr = dave.local_addr().await.unwrap();
    let ghost_addr = ghost.local_addr().await.unwrap();
    alice.connect(carol_addr).await.unwrap();
    alice.connect(dave_addr).await.unwrap();
    alice.connect(ghost_addr).await.unwrap();
    assert!(wait_connected(&alice, carol_addr).await);
    assert!(wait_connected(&alice, dave_addr).await);
    assert!(wait_connected(&alice, ghost_addr).await);

    // Disconnect ghost, then flood the high lane with unicasts addressed
    // to it. Every tick that drains one of these finds the target gone.
    alice.disconnect(ghost_addr).await;
    let marker = b"peashape-ghost-unicast-marker";
    for _ in 0..200 {
        // Lane saturates (bounded); we just want it kept non-empty.
        let _ = alice.send_shaped(ghost_addr, marker);
    }

    let mut carol_rx = carol.subscribe();
    let mut dave_rx = dave.subscribe();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut leaked = false;
    while !leaked && Instant::now() < deadline {
        tokio::select! {
            r = tokio::time::timeout(Duration::from_millis(100), carol_rx.recv()) => {
                if let Ok(Ok(buf)) = r {
                    if contains_payload(&buf, marker) { leaked = true; }
                }
            }
            r = tokio::time::timeout(Duration::from_millis(100), dave_rx.recv()) => {
                if let Ok(Ok(buf)) = r {
                    if contains_payload(&buf, marker) { leaked = true; }
                }
            }
        }
    }

    assert!(
        !leaked,
        "a unicast to a disconnected peer leaked to a non-target peer; \
         it must be dropped and replaced with cover"
    );

    alice.shutdown().await;
    carol.shutdown().await;
    dave.shutdown().await;
    ghost.shutdown().await;
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
async fn per_connection_unicast_reaches_only_target() {
    // In PerConnection scope a unicast is queued on the *target peer's
    // own* lane and drained on that peer's round-robin slot, so it
    // reaches the intended peer and no other. A non-target peer sees
    // only cover, which never matches the marker.
    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(25),
        },
        ShapingScope::PerConnection { randomize: false },
    );
    alice_config.fanout = 1;
    let alice = Node::new(alice_config);

    let mk = |name: &str| {
        let mut cfg = test_config(
            name,
            ShapingStrategy::Constant {
                interval: Duration::from_secs(10),
            },
            ShapingScope::Global,
        );
        cfg.frame_size = 128;
        Node::new(cfg)
    };
    let bob = mk("bob");
    let carol = mk("carol");

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    carol.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    let carol_addr = carol.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    alice.connect(carol_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);
    assert!(wait_connected(&alice, carol_addr).await);

    let mut bob_rx = bob.subscribe();
    let mut carol_rx = carol.subscribe();
    let marker = b"peashape-pc-unicast-marker";
    alice.send_shaped(bob_addr, marker).expect("send_shaped");

    // Run the full window so carol has many round-robin slots in which
    // she could (wrongly) receive the marker.
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut bob_got = false;
    let mut carol_got = false;
    while Instant::now() < deadline {
        tokio::select! {
            r = tokio::time::timeout(Duration::from_millis(100), bob_rx.recv()) => {
                if let Ok(Ok(buf)) = r {
                    if contains_payload(&buf, marker) { bob_got = true; }
                }
            }
            r = tokio::time::timeout(Duration::from_millis(100), carol_rx.recv()) => {
                if let Ok(Ok(buf)) = r {
                    if contains_payload(&buf, marker) { carol_got = true; }
                }
            }
        }
    }
    assert!(
        bob_got,
        "target peer never received the PerConnection unicast"
    );
    assert!(
        !carol_got,
        "a non-target peer received a PerConnection unicast"
    );

    alice.shutdown().await;
    bob.shutdown().await;
    carol.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_connection_broadcast_reaches_all_peers() {
    // In PerConnection scope `broadcast_shaped` fans a copy of the
    // frame into every currently-connected peer's lane, so every peer
    // receives it on its own shaped slot.
    let alice = Node::new(test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(25),
        },
        ShapingScope::PerConnection { randomize: false },
    ));

    let mk = |name: &str| {
        let mut cfg = test_config(
            name,
            ShapingStrategy::Constant {
                interval: Duration::from_secs(10),
            },
            ShapingScope::Global,
        );
        cfg.frame_size = 128;
        Node::new(cfg)
    };
    let bob = mk("bob");
    let carol = mk("carol");

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    carol.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    let carol_addr = carol.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    alice.connect(carol_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);
    assert!(wait_connected(&alice, carol_addr).await);

    let mut bob_rx = bob.subscribe();
    let mut carol_rx = carol.subscribe();
    // The broadcast fan-out consults the scheduler-maintained peer
    // cache; give it a couple of ticks to learn about both peers.
    tokio::time::sleep(Duration::from_millis(60)).await;
    let marker = b"peashape-pc-broadcast-marker";
    alice.broadcast_shaped(marker).expect("broadcast_shaped");

    let deadline = Instant::now() + Duration::from_secs(1);
    let mut bob_got = false;
    let mut carol_got = false;
    while !(bob_got && carol_got) && Instant::now() < deadline {
        tokio::select! {
            r = tokio::time::timeout(Duration::from_millis(100), bob_rx.recv()) => {
                if let Ok(Ok(buf)) = r {
                    if contains_payload(&buf, marker) { bob_got = true; }
                }
            }
            r = tokio::time::timeout(Duration::from_millis(100), carol_rx.recv()) => {
                if let Ok(Ok(buf)) = r {
                    if contains_payload(&buf, marker) { carol_got = true; }
                }
            }
        }
    }
    assert!(
        bob_got && carol_got,
        "PerConnection broadcast did not reach all peers (bob={bob_got}, carol={carol_got})"
    );

    alice.shutdown().await;
    bob.shutdown().await;
    carol.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn custom_cover_generator_is_used() {
    // A node configured with a custom cover generator must emit that
    // generator's frames (not uniform random cover) when its lanes are
    // empty. We stamp every cover frame with a recognizable marker and
    // assert the peer sees it; with no real traffic, every frame is
    // cover.
    let marker = b"COVER-GENERATOR-MARKER".to_vec();
    let stamp = marker.clone();
    let gen: std::sync::Arc<dyn CoverGenerator> =
        std::sync::Arc::new(move |config: &ShapeConfig| {
            let mut f = BytesMut::with_capacity(config.frame_size);
            f.extend_from_slice(&stamp);
            f.resize(config.frame_size, 0);
            f
        });

    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    );
    alice_config.cover_generator = Some(gen);
    let alice = Node::new(alice_config);
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

    let mut rx = bob.subscribe();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut got = false;
    while !got && Instant::now() < deadline {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            // The custom generator's marker is at the front of the frame.
            if buf.starts_with(&marker[..]) {
                got = true;
            }
        }
    }
    assert!(
        got,
        "peer never saw a frame from the custom cover generator"
    );

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cover_generator_finalizes_real_lane_frames() {
    // A generator's `finalize_real` hook must be applied to real frames
    // drained from the priority lanes (not just to cover frames). We
    // stamp a recognizable marker onto the front of every real frame in
    // `finalize_real`; cover frames are left blank. The peer must see a
    // frame carrying the marker, proving a lane frame went through the
    // hook.
    struct Stamper(Vec<u8>);
    impl CoverGenerator for Stamper {
        fn cover(&self, config: &ShapeConfig) -> BytesMut {
            let mut f = BytesMut::with_capacity(config.frame_size);
            f.resize(config.frame_size, 0);
            f
        }
        fn finalize_real(&self, _config: &ShapeConfig, mut frame: BytesMut) -> BytesMut {
            let n = self.0.len().min(frame.len());
            frame[..n].copy_from_slice(&self.0[..n]);
            frame
        }
    }

    let marker = b"FINALIZED-REAL-FRAME".to_vec();
    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    );
    alice_config.cover_generator = Some(std::sync::Arc::new(Stamper(marker.clone())));
    let alice = Node::new(alice_config);
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

    let mut rx = bob.subscribe();
    // Submit a real frame through the high-priority lane; it must be
    // drained and routed through `finalize_real` before hitting the wire.
    alice.broadcast_shaped(b"real payload").expect("broadcast");

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut got = false;
    while !got && Instant::now() < deadline {
        if let Ok(Ok(buf)) = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            if buf.starts_with(&marker[..]) {
                got = true;
            }
        }
    }
    assert!(got, "a lane frame was never routed through finalize_real");

    alice.shutdown().await;
    bob.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[should_panic(expected = "at least ID_SIZE")]
async fn frame_size_below_id_size_panics() {
    // A frame must be able to hold its ID prefix; constructing a node
    // with `frame_size < ID_SIZE` must fail loudly rather than letting
    // the framing helpers underflow at runtime.
    let mut config = test_config(
        "node",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(50),
        },
        ShapingScope::Global,
    );
    config.frame_size = ID_SIZE - 1;
    let _ = Node::new(config);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_connection_unicast_does_not_starve_other_links() {
    // Regression test for the per-tick frame-count invariant under
    // `Global` scope: a tick that drains a real *unicast* frame must
    // still put `fanout` frames on the wire (the real one to the
    // recipient, cover to the rest), exactly like a cover or
    // broadcast tick. If unicast ticks emitted only a single frame,
    // a passive observer counting frames per tick could pick out
    // precisely which ticks carried real unicast traffic — and to
    // whom. Concretely: if Alice spends every tick sending a unicast
    // to Bob, the *other* peers (Carol, Dave) must keep receiving
    // cover at the full shaping rate rather than being starved.
    let mut alice_config = test_config(
        "alice",
        ShapingStrategy::Constant {
            interval: Duration::from_millis(25),
        },
        ShapingScope::Global,
    );
    alice_config.fanout = 3;
    let alice = Node::new(alice_config);

    // The receivers shape very slowly so their own cover traffic
    // does not pollute the measurement; what we count at Carol and
    // Dave is essentially all driven by Alice's schedule.
    let mk_receiver = |name: &str| {
        let mut cfg = test_config(
            name,
            ShapingStrategy::Constant {
                interval: Duration::from_secs(10),
            },
            ShapingScope::Global,
        );
        cfg.frame_size = 128; // match alice (test_config default)
        Node::new(cfg)
    };
    let bob = mk_receiver("bob");
    let carol = mk_receiver("carol");
    let dave = mk_receiver("dave");

    alice.spawn().await.unwrap();
    bob.spawn().await.unwrap();
    carol.spawn().await.unwrap();
    dave.spawn().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let bob_addr = bob.local_addr().await.unwrap();
    let carol_addr = carol.local_addr().await.unwrap();
    let dave_addr = dave.local_addr().await.unwrap();
    alice.connect(bob_addr).await.unwrap();
    alice.connect(carol_addr).await.unwrap();
    alice.connect(dave_addr).await.unwrap();
    assert!(wait_connected(&alice, bob_addr).await);
    assert!(wait_connected(&alice, carol_addr).await);
    assert!(wait_connected(&alice, dave_addr).await);

    // Keep the high lane full of Bob-targeted unicasts so that
    // essentially every tick in the measurement window drains a
    // real unicast (not a fallback cover broadcast).
    for _ in 0..120 {
        // Ignore LaneFull once the (bounded) lane saturates; we just
        // want it kept non-empty across the window.
        let _ = alice.send_shaped(bob_addr, b"unicast-to-bob");
    }

    let mut carol_rx = carol.subscribe();
    let mut dave_rx = dave.subscribe();
    let start = Instant::now();
    let mut carol_count = 0usize;
    let mut dave_count = 0usize;
    while start.elapsed() < Duration::from_millis(1500) {
        tokio::select! {
            r = tokio::time::timeout(Duration::from_millis(100), carol_rx.recv()) => {
                if r.is_ok() { carol_count += 1; }
            }
            r = tokio::time::timeout(Duration::from_millis(100), dave_rx.recv()) => {
                if r.is_ok() { dave_count += 1; }
            }
        }
    }

    // With a 25 ms interval over ~1.5 s there are ~60 ticks. Under
    // the fixed behavior each unicast tick also sends cover to both
    // non-target peers, so Carol and Dave should each see many
    // frames. Under the buggy behavior (unicast tick -> Bob only)
    // they would each see ~0. A generous lower bound cleanly
    // separates the two.
    assert!(
        carol_count >= 10,
        "carol was starved of cover while alice unicast to bob (got {carol_count}); \
         a Global unicast tick must still emit fanout frames"
    );
    assert!(
        dave_count >= 10,
        "dave was starved of cover while alice unicast to bob (got {dave_count}); \
         a Global unicast tick must still emit fanout frames"
    );

    alice.shutdown().await;
    bob.shutdown().await;
    carol.shutdown().await;
    dave.shutdown().await;
}
