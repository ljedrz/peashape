# peashape

A traffic-shaping middleware for [`pea2pea`] nodes. It
enforces two simple rules on every outbound frame:

1. **Constant size.** Every frame on the wire is padded (or
   covered) to a single fixed `frame_size`, so an observer
   cannot distinguish "what" is being sent by length.
2. **Constant (or Poisson) timing.** A background scheduler
   ticks at a configured rate; on every tick it pulls the next
   real message from a priority lane, or generates a cover
   message if the lanes are empty.

Real and cover messages share the same code path, so the
resulting outbound traffic is observationally indistinguishable
from a stream with no real content at all.

`peashape` is a building block: use it as a substrate for a
private RPC system, a private pub/sub layer, a private
membership protocol, or — in conjunction with [`peasub`] — a
metadata-private gossip network. Running `peashape` on every
hop of a multi-hop network gives "cover on top of cover": every
frame is re-padded and re-timed by the local shaper as it
passes through, so a passive observer who can only watch one
link still cannot correlate traffic across hops.

## Quick start

```rust
use std::time::Duration;
use peashape::{Node, ShapeConfig, ShapingStrategy};

let node = Node::new(ShapeConfig {
    name: Some("alice".into()),
    listener_addr: Some("127.0.0.1:0".parse()?),
    strategy: ShapingStrategy::Constant {
        interval: Duration::from_millis(100),
    },
    ..Default::default()
});

let _addr = node.spawn().await?;

// Subscribe to incoming frames (real and cover alike).
let mut rx = node.subscribe();

// Send a real message to a specific peer (after `connect(addr)`).
node.send_shaped(peer, b"hello, peashape")?;

// Or broadcast it; the scheduler fans it out to `fanout` peers.
node.broadcast_shaped(b"hello, world")?;

# drop(rx);
node.shutdown().await;
# Ok::<_, Box<dyn std::error::Error>>(())
```

Run `cargo run --example demo` to see the
traffic-analysis-resistance property visualized, or
`cargo run --example two_nodes` for the minimal end-to-end
usage pattern.

## Priority lanes

`peashape` exposes two priority lanes for application traffic:

- `Lane::High` — FIFO, always drained first. The natural
  choice for application-submitted traffic: a fresh
  `broadcast_shaped(...)` is sent on the very next shaping
  tick regardless of how much other traffic has piled up.
  Bounded; once full, further enqueues return
  `Error::LaneFull`.
- `Lane::Low` — LIFO with drop-oldest eviction. The natural
  choice for relay traffic: a freshly-enqueued frame is
  pushed to the front and the very next shaping tick pops
  it without waiting in line behind older relays.

The shaping scheduler always drains the high lane before the
low lane, and falls back to a freshly-generated cover frame
when both are empty.

## Shaping strategies

```rust
pub enum ShapingStrategy {
    /// One frame per fixed `interval`.
    Constant { interval: Duration },
    /// Inter-arrival times drawn from Exp(rate) - Poisson process.
    Poisson { rate: f64 },
}
```

`Constant` is the simplest and most predictable. `Poisson`
makes the inter-arrival times themselves random, so an
observer who can only see *the node's* traffic cannot
statistically distinguish the Poisson-scheduled cover stream
from one whose timing is influenced by user activity — the
strictest form of metadata privacy.

## Scopes: global vs. per-connection

`peashape` supports two scheduling scopes:

- `ShapingScope::Global` (default) — one global ticker;
  every tick broadcasts the next frame to `fanout` random
  connected peers. This is the natural choice for
  gossip-style protocols where a single message goes to
  many destinations.
- `ShapingScope::PerConnection` — one ticker per connection;
  every tick sends one frame to a single round-robin
  peer. `fanout` is ignored in this mode.

## Unicast and broadcast

Two complementary submit methods are provided:

- `Node::send_shaped(peer, payload)` — sends to one specific
  peer (or silently drops the frame if the peer is no longer
  connected at tick time).
- `Node::broadcast_shaped(payload)` — fanout-based broadcast
  (in `Global` mode) or round-robin (in `PerConnection`).

The same pair of methods is available for the *low*-priority
lane: `Node::send_shaped_low(peer, payload)` and
`Node::broadcast_shaped_low(payload)`. And for re-broadcasting
a pre-built frame byte-for-byte (e.g. a frame received from
a peer that you want to relay unchanged):
`Node::relay_shaped(frame)`.

## Threat model

`peashape` is designed to defeat a *passive global network
observer* who can:

- observe every byte sent between every pair of nodes;
- observe the timing of every byte;
- but cannot break the cryptographic primitives protecting
  the link (e.g. TLS via a `pea2pea` `Handshake`).

Against such an observer, the shaping schedule ensures that
the *timing distribution* and *size distribution* of a
node's outbound traffic are independent of whether the
application is submitting messages or not. The observer
learns nothing about the existence, frequency, or
destination of user activity beyond the rate the node has
been configured for.

`peashape` does **not** attempt to defeat an observer that
can compromise the node itself, that can correlate
application-level events with coarse traffic features, or
that controls a non-trivial fraction of the network's nodes.

For the full threat model, see the crate-level documentation.

## Choosing the shaping rate

The shaping rate is the *only* knob that controls the privacy
/ bandwidth trade-off:

- **Higher** shaping rate → tighter privacy, more bandwidth.
- **Lower** shaping rate → looser privacy, less bandwidth.

The application must submit no faster than the shaping rate;
otherwise its messages accumulate in the high-priority lane.
The `high_lane_capacity` should be sized to
`shaping_rate * burst_seconds` to keep submission latency low
under reasonable bursts.

## Composition with `peasub`

`peasub` is a metadata-private gossip protocol built on top of
`peashape`. The `peasub::Node` wraps a `peashape::Node` and
adds:

- an LRU of recently-seen message identifiers (for dedup);
- a background task that subscribes to the `peashape` incoming
  broadcast, dedups each frame against the LRU, and re-enqueues
  novel frames into `peashape`'s low-priority lane for fanout
  (LIFO discipline forwards a fresh relay on the next tick).

`peasub` uses `peashape`'s `broadcast_shaped` to put bytes on
the wire, and `peashape`'s `subscribe` channel to receive
them. The result: every byte that hits the wire is shaped by
`peashape`, so `peasub` inherits the metadata-privacy property
for free.

[`pea2pea`]: https://docs.rs/pea2pea
[`peasub`]: https://docs.rs/peasub

## License

Dual-licensed under MIT or CC0-1.0, at your option.
