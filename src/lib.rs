//! `peashape` — a traffic-shaping middleware for [`pea2pea`]
//! nodes.
//!
//! # What it does
//!
//! `peashape` is a thin layer on top of [`pea2pea`] that
//! enforces two simple rules on every outbound frame:
//!
//! 1. **The frame size is constant.** Every byte the node
//!    writes to the wire is padded (or covered) to exactly
//!    `frame_size` bytes, so an observer cannot distinguish
//!    "what" is being sent by length.
//! 2. **The frame timing is constant (or Poisson).** A
//!    background scheduler ticks at a configured rate; on every
//!    tick it pulls the next real message from a bounded
//!    priority lane, or generates a cover message if the lanes
//!    are empty. Because real and cover messages share the same
//!    code path, and because the *timing* of the ticks is
//!    independent of application activity, the resulting
//!    outbound traffic is observationally indistinguishable from
//!    a stream with no real content at all.
//!
//! The two rules together defeat a *passive global network
//! observer* who can see every byte sent between every pair of
//! nodes, and the timing of every byte, but cannot break the
//! cryptographic primitives protecting the link. The observer
//! learns nothing about the existence, frequency, or
//! destination of user activity beyond the rate the node has
//! been configured for.
//!
//! # Quick start
//!
//! ```no_run
//! use std::time::Duration;
//! use peashape::{Node, ShapeConfig, ShapingStrategy};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let node = Node::new(ShapeConfig {
//!     name: Some("alice".into()),
//!     listener_addr: Some("127.0.0.1:0".parse()?),
//!     strategy: ShapingStrategy::Constant {
//!         interval: Duration::from_millis(100),
//!     },
//!     ..Default::default()
//! });
//!
//! let _local_addr = node.spawn().await?;
//!
//! // subscribe to incoming frames
//! let mut rx = node.subscribe();
//!
//! // submit a real message to a peer (call `node.connect(addr)` first)
//! // node.send_shaped(peer, b"hello, peashape")?;
//!
//! // or broadcast it: the scheduler will fan it out to `fanout` peers
//! // node.broadcast_shaped(b"hello, world")?;
//! # drop(rx);
//! node.shutdown().await;
//! # Ok(()) }
//! ```
//!
//! # When to use `peashape`
//!
//! - **Standalone**: any `pea2pea` application that needs a
//!   constant-rate or Poisson outbound stream with cover
//!   traffic — for example, a private RPC system that wants to
//!   hide *when* a user makes a request.
//! - **As a substrate**: a higher-level protocol (e.g. a
//!   gossip layer, a publish-subscribe system, a private
//!   membership protocol) that wants to inherit the
//!   metadata-privacy property for free. The application calls
//!   [`Node::send_shaped`] / [`Node::broadcast_shaped`] instead
//!   of raw `unicast` / `broadcast`, and the rest of the
//!   protocol — framing, scheduling, cover — is taken care of.
//! - **Stacked**: running `peashape` on every hop of a
//!   multi-hop network gives "cover on top of cover": every
//!   frame is re-padded and re-timed by the local shaper as it
//!   passes through, so a passive observer who can only watch
//!   one link still cannot correlate traffic across hops.
//!
//! # Threat model
//!
//! `peashape` is designed to defeat a *passive global network
//! observer* who can:
//!
//! - observe every byte sent between every pair of nodes;
//! - observe the timing of every byte;
//! - but cannot break the cryptographic primitives protecting
//!   the link (e.g. TLS via a `pea2pea` `Handshake`).
//!
//! Against such an observer, the shaping schedule ensures that
//! the *timing distribution* and *size distribution* of a
//! node's outbound traffic are independent of whether the
//! application is submitting messages or not. The observer
//! learns nothing about the existence, frequency, or
//! destination of user activity beyond the rate the node has
//! been configured for.
//!
//! The one caveat is *destination* under sustained **unicast**
//! traffic in [`ShapingScope::Global`]: a peer that is the
//! steady recipient of real unicast frames receives them at a
//! marginally higher long-run rate than its cover-only share,
//! a residual aggregate signal inherent to carrying unicast
//! over a gossip-style fanout (see [`Scheduler`]). Broadcast
//! traffic spreads uniformly and is unaffected, and
//! [`ShapingScope::PerConnection`] removes the signal entirely
//! — a real frame merely occupies the cover slot the
//! recipient's link was going to emit anyway. Use
//! `PerConnection` for unicast-heavy workloads.
//!
//! `peashape` does **not** attempt to defeat:
//!
//! - an observer that can compromise the node itself;
//! - side channels outside the network (e.g. a screen-snooping
//!   adversary, or a process that visibly burns CPU only when
//!   the user is active);
//! - an observer that controls a non-trivial fraction of the
//!   network's nodes and can correlate across them;
//! - traffic *content* analysis: cover hides *when* messages
//!   are sent, not *what* they say. End-to-end payload
//!   confidentiality is the application's responsibility (or
//!   can be layered on via a `pea2pea` `Handshake`).
//!
//! # Architecture
//!
//! - [`Node`] wraps a [`pea2pea::Node`] and adds three pieces of
//!   bookkeeping: a bounded *high-priority lane* (FIFO; always
//!   drained first), a bounded *low-priority lane* (LIFO with
//!   drop-oldest), a broadcast channel of incoming frames, and
//!   a background [`Scheduler`] task that drains the lanes at
//!   the configured rate.
//! - The length-delimited [`Codec`] forces every frame on the
//!   wire to a single fixed size (configurable via
//!   [`ShapeConfig::frame_size`]), so the *length* of a frame
//!   is never a tell.
//! - The first [`ID_SIZE`] bytes of every frame are a random
//!   message identifier (per the convention, but `peashape`
//!   itself does not interpret them). Real messages receive a
//!   random ID at submission time; cover messages receive a
//!   fresh random ID per emission. The receiver's
//!   [`subscribe`](Node::subscribe) channel yields every frame
//!   it sees on the wire; the application filters for real
//!   traffic (typically by authenticating-decrypting with a
//!   recognizable structure that random cover bytes won't
//!   match).
//! - When a tick fires, the scheduler either pulls from the
//!   high-priority lane, then the low-priority lane, or
//!   generates a cover frame — and ships it to the right
//!   peer(s) according to the configured [`ShapingScope`]. Every
//!   tick emits exactly the same number of frames of the same
//!   on-the-wire size, so the metadata-privacy property is
//!   preserved regardless of `fanout`.
//!
//! # Composing with other protocols
//!
//! `peashape` is designed to be a building block. The most
//! common composition is to wrap it inside a higher-level
//! protocol: the higher-level layer is in charge of the
//! application semantics (e.g. gossip, pub/sub, RPC), and
//! uses `peashape`'s [`Node::send_shaped`] /
//! [`Node::broadcast_shaped`] to actually put bytes on the
//! wire. Because the wire format is "constant-size
//! length-delimited frames at a constant (or Poisson) rate,"
//! the higher-level protocol inherits the metadata-privacy
//! property for free.
//!
//! For multi-hop networks, run `peashape` on every node in the
//! network; each hop independently adds its own cover traffic,
//! so a passive observer who can only watch a single link still
//! cannot correlate traffic across hops. This is the
//! "cover on top of cover" property.
//!
//! # Choosing the shaping rate
//!
//! The shaping rate is the *only* knob that controls the
//! privacy / bandwidth trade-off. As a rule of thumb:
//!
//! - the application should submit no faster than the
//!   configured rate, otherwise its messages accumulate in
//!   the high-priority lane;
//! - `high_lane_capacity` should be sized to
//!   `rate * burst_seconds` to keep submission latency low
//!   under reasonable bursts;
//! - `low_lane_capacity` should be sized to the expected
//!   burst of relay traffic (e.g.
//!   `fanout * cover_rate * drain_seconds` for a gossip
//!   layer on top of `peashape`).
//!
//! [`pea2pea`]: pea2pea

#![deny(unsafe_code)]
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

mod codec;
mod config;
mod error;
mod frame;
mod node;
mod scheduler;
mod shaper;

pub use crate::codec::Codec;
pub use crate::config::{CoverGenerator, Lane, ShapeConfig, ShapingScope, ShapingStrategy};
pub use crate::error::Error;
pub use crate::frame::{build_frame, random_cover, ID_SIZE};
pub use crate::node::Node;
pub use crate::scheduler::Scheduler;
pub use crate::shaper::{PendingFrame, Shaper, Target, SUBSCRIBER_CAPACITY};

/// Re-exported so that callers can wire up a topology in tests
/// without adding `pea2pea` as a direct dependency.
pub use pea2pea::{self, connect_nodes, Topology};
