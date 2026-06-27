//! Configuration types for a [`Node`].
//!
//! [`Node`]: crate::Node

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;

/// Generates cover frames for a [`Node`] when its priority lanes are
/// empty at a shaping tick.
///
/// The default generator ([`random_cover`]) fills each cover frame
/// with uniform random bytes. Supply a custom generator via
/// [`ShapeConfig::cover_generator`] to make cover traffic mimic a
/// specific wire profile — for example, valid RTP packets, so that
/// the *whole* stream (real frames and cover alike) looks like a
/// media call to a passive observer rather than a mix of structured
/// and random bytes.
///
/// # Contract
///
/// - `cover` is called from the scheduler on a tick whose lanes are
///   empty, so it must be cheap and must not block.
/// - It **must** return exactly `config.frame_size` bytes. A frame
///   of any other size is rejected by the codec and silently
///   dropped, which would break the constant-rate property.
/// - Implementations are shared across threads and invoked through a
///   shared reference, so any per-frame state (e.g. an RTP sequence
///   counter) needs interior mutability (an `AtomicU16`, a `Mutex`,
///   …).
///
/// A bare closure `Fn(&ShapeConfig) -> BytesMut` implements this
/// trait, so trivial generators need no custom type.
///
/// [`random_cover`]: crate::random_cover
/// [`Node`]: crate::Node
pub trait CoverGenerator: Send + Sync {
    /// Returns a single freshly-generated cover frame of exactly
    /// `config.frame_size` bytes.
    fn cover(&self, config: &ShapeConfig) -> BytesMut;

    /// Finalizes a *real* (application-submitted, lane-drained) frame
    /// just before it is written to the wire — called in send order,
    /// on the same tick cadence as [`cover`](CoverGenerator::cover).
    ///
    /// The default returns the frame unchanged. Override it to fold
    /// real frames into the *same* stream as cover — for example, to
    /// stamp a shared RTP sequence number and timestamp — so that a
    /// passive observer sees one coherent stream rather than two
    /// independently-sequenced producers (the priority lanes and the
    /// cover generator). This is what lets a node carry real traffic
    /// over the priority lanes while still presenting a single,
    /// in-order stream, which matters on connection-oriented
    /// transports.
    ///
    /// Like [`cover`](CoverGenerator::cover), it must return exactly
    /// `config.frame_size` bytes, and is invoked through a shared
    /// reference, so any shared sequencing state needs interior
    /// mutability. Coherent single-stream sequencing is a
    /// [`PerConnection`](crate::ShapingScope::PerConnection) concept:
    /// one stream per peer.
    fn finalize_real(&self, config: &ShapeConfig, frame: BytesMut) -> BytesMut {
        let _ = config;
        frame
    }
}

impl<F> CoverGenerator for F
where
    F: Fn(&ShapeConfig) -> BytesMut + Send + Sync,
{
    fn cover(&self, config: &ShapeConfig) -> BytesMut {
        self(config)
    }
}

/// The set of parameters that govern a [`Node`].
///
/// Most callers only need to set [`ShapeConfig::strategy`]; every
/// other field has a sensible default. To see what the defaults
/// are, use [`ShapeConfig::default`] and read the resulting
/// struct.
///
/// [`Node`]: crate::Node
#[derive(Clone)]
pub struct ShapeConfig {
    /// A friendly identifier of the node, surfaced in `tracing`
    /// output. If `None`, `pea2pea` assigns a numeric ID.
    pub name: Option<String>,

    /// The local socket address to bind to for inbound connections.
    /// If `None`, the node will not accept inbound connections (it
    /// can still initiate outbound ones via [`Node::connect`]).
    ///
    /// [`Node::connect`]: crate::Node::connect
    pub listener_addr: Option<std::net::SocketAddr>,

    /// The strategy used to schedule outgoing traffic. This is the
    /// central knob controlling the metadata-privacy properties
    /// of the node; see [`ShapingStrategy`] for the two options.
    pub strategy: ShapingStrategy,

    /// Whether the shaping schedule ticks once for the whole node
    /// (broadcasting to `fanout` random peers per tick), or once
    /// per connection (one frame per peer per tick, round-robin).
    pub scope: ShapingScope,

    /// Number of distinct peers each outbound frame is forwarded
    /// to on every cover tick. Higher fanout → faster propagation
    /// at the cost of proportionally more bandwidth.
    ///
    /// `fanout = 1` preserves a simple single-target per-tick
    /// behavior; production deployments typically use 3–6 so a
    /// single message reaches the whole overlay in `O(log N)`
    /// hops rather than `O(N)` hops.
    ///
    /// The effective fanout is clamped to the number of connected
    /// peers on every tick, so a node with fewer peers than
    /// `fanout` simply sends to all of them.
    ///
    /// # Bandwidth
    ///
    /// Total outbound bandwidth is `fanout * frame_size * rate`.
    /// Raising `fanout` raises bandwidth linearly but does **not**
    /// change the timing distribution of outbound traffic (every
    /// tick still emits exactly `fanout` frames, real or cover),
    /// so the metadata-privacy property is preserved.
    ///
    /// # `PerConnection` scope
    ///
    /// In [`ShapingScope::PerConnection`] mode, the per-tick
    /// selection is just one peer; `fanout` is irrelevant (clamped
    /// to 1).
    pub fanout: usize,

    /// The on-the-wire payload size, in bytes, of every frame.
    ///
    /// All frames (real and cover) are padded to exactly this
    /// size, so an observer cannot distinguish the two by length.
    ///
    /// Must be greater than zero and at most `max_frame_size`.
    pub frame_size: usize,

    /// Maximum number of frames in the *high*-priority lane.
    ///
    /// The high-priority lane is a FIFO that the scheduler
    /// always drains first. It is the natural choice for
    /// application-submitted traffic: the next shaping tick
    /// transmits a high-priority frame regardless of how much
    /// other traffic has piled up. Once the lane is full,
    /// further enqueues return [`Error::LaneFull`].
    ///
    /// [`Error::LaneFull`]: crate::Error::LaneFull
    pub high_lane_capacity: usize,

    /// Maximum number of frames in the *low*-priority lane.
    ///
    /// The low-priority lane is a LIFO with drop-oldest
    /// eviction: a freshly-enqueued frame is pushed to the
    /// front, and the scheduler pops the front. Under sustained
    /// inflow, the oldest queued frame is discarded from the
    /// back to make room. This is the natural choice for
    /// *relay* traffic: a fresh relay goes to the front so the
    /// very next shaping tick forwards it without waiting in
    /// line behind older relays. Once the lane is full, the
    /// oldest frame is silently evicted (and the new one is
    /// accepted).
    pub low_lane_capacity: usize,

    /// Upper bound, in bytes, on a single frame the decoder will
    /// accept. Frames larger than this are rejected and the
    /// connection is torn down by `pea2pea`. The configured
    /// `frame_size` must not exceed this.
    pub max_frame_size: usize,

    /// Maximum number of simultaneously-active connections.
    pub max_connections: u16,

    /// Maximum number of connections to a single IP address. The
    /// `pea2pea` default of `1` is too restrictive for typical
    /// tests (every node typically has at least one connection to
    /// each of its peers, which all share the same IP in loopback
    /// tests), so this defaults to `8`.
    pub max_connections_per_ip: u16,

    /// Whether to set `SO_REUSEPORT` on the listener socket, which
    /// allows multiple `peashape` nodes to bind the same address
    /// simultaneously and have the kernel load-balance inbound
    /// connections across them. Useful for zero-downtime upgrades
    /// and sharded listeners. Has no effect on platforms that do
    /// not support `SO_REUSEPORT` (in which case the listener
    /// fails to bind).
    pub reuse_listener_port: bool,

    /// An optional custom cover-frame generator. When `None` (the
    /// default), cover frames are uniform random bytes
    /// ([`random_cover`]). Supply one to make cover traffic mimic a
    /// specific wire profile (e.g. RTP packets); see
    /// [`CoverGenerator`].
    ///
    /// [`random_cover`]: crate::random_cover
    pub cover_generator: Option<Arc<dyn CoverGenerator>>,
}

impl Default for ShapeConfig {
    fn default() -> Self {
        Self {
            name: None,
            listener_addr: None,
            // 1 message/second is the most conservative default; the
            // operator should raise this in line with their expected
            // publish rate.
            strategy: ShapingStrategy::Constant {
                interval: Duration::from_secs(1),
            },
            scope: ShapingScope::Global,
            // 3 is the standard gossip-sub fanout: enough for
            // O(log N) convergence in typical overlays, modest
            // enough that the default 1 s cover interval stays
            // cheap. Raise it for denser overlays or tighter
            // convergence requirements.
            fanout: 3,
            // 256 bytes is a reasonable default for small messages:
            // big enough to carry a short payload with an ID prefix,
            // small enough to keep per-connection memory low.
            frame_size: 256,
            // 256 high-priority frames ≈ 4 minutes of slack at
            // 1 msg/s, enough for any reasonable burst.
            high_lane_capacity: 256,
            // 1024 low-priority slots — with the LIFO discipline
            // only the most recent frames survive, so this is
            // mostly relevant under very high relay inflow.
            low_lane_capacity: 1024,
            max_frame_size: 1024 * 1024,
            max_connections: 64,
            max_connections_per_ip: 8,
            reuse_listener_port: false,
            cover_generator: None,
        }
    }
}

impl std::fmt::Debug for ShapeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShapeConfig")
            .field("name", &self.name)
            .field("listener_addr", &self.listener_addr)
            .field("strategy", &self.strategy)
            .field("scope", &self.scope)
            .field("fanout", &self.fanout)
            .field("frame_size", &self.frame_size)
            .field("high_lane_capacity", &self.high_lane_capacity)
            .field("low_lane_capacity", &self.low_lane_capacity)
            .field("max_frame_size", &self.max_frame_size)
            .field("max_connections", &self.max_connections)
            .field("max_connections_per_ip", &self.max_connections_per_ip)
            .field("reuse_listener_port", &self.reuse_listener_port)
            // The generator is opaque; report only its presence.
            .field(
                "cover_generator",
                &self.cover_generator.as_ref().map(|_| "<custom>"),
            )
            .finish()
    }
}

/// How the node generates outbound traffic.
///
/// The two strategies differ only in the inter-arrival timing of
/// cover messages; the *total* outgoing rate (and the
/// indistinguishability of real vs. cover frames on the wire) is
/// preserved.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum ShapingStrategy {
    /// Emit one cover frame exactly every `interval`.
    ///
    /// The simplest and most predictable schedule. The outgoing
    /// bandwidth is `frame_size / interval` per peer-pair;
    /// suitable when peers have a loose real-time sync and the
    /// cover rate is not too high.
    Constant {
        /// The fixed delay between consecutive cover frames.
        interval: Duration,
    },

    /// Emit cover frames with inter-arrival times drawn from
    /// `Exp(rate)`, i.e. a Poisson process with mean inter-arrival
    /// time `1 / rate` seconds.
    ///
    /// Because the schedule itself is randomized, an observer who
    /// can only see *the node's* traffic cannot statistically
    /// distinguish a Poisson-scheduled cover stream from a stream
    /// whose inter-arrival times are influenced by user activity.
    /// This is the "metadata-private" choice in the strictest
    /// sense: the observed process is itself a Poisson process
    /// regardless of what the application does.
    Poisson {
        /// The Poisson-process rate, in frames per second.
        rate: f64,
    },

    /// No scheduling at all: no cover traffic, no constant-rate
    /// emission. Real (application-submitted) frames are sent as
    /// soon as they are enqueued, at the byte size the application
    /// supplied (the application is still responsible for padding
    /// to `frame_size` if it needs on-the-wire uniformity).
    ///
    /// Use this when the protocol above `peashape` does not need
    /// metadata-privacy properties (constant rate, constant size)
    /// and would rather not pay the cover-traffic bandwidth cost.
    /// Both ends of a connection must use the same strategy; the
    /// receive side still enforces `frame_size`, so both ends
    /// should be in `None` and agree on `frame_size`.
    None,
}

/// Whether the scheduler ticks once for the whole node, or once
/// per connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ShapingScope {
    /// One global ticker; every tick broadcasts the next frame to
    /// `fanout` random peers. This is the natural choice for
    /// gossip-style protocols where a single message goes to many
    /// destinations.
    Global,

    /// A single node-wide ticker that serves one peer per tick.
    ///
    /// On each tick the scheduler selects one connected peer
    /// (round-robin, optionally mixed with a per-tick random offset)
    /// and sends it exactly one frame: a real frame drawn from
    /// *that peer's own* priority lanes if one is waiting, otherwise
    /// cover. Each peer therefore sees a steady, application-
    /// independent stream — a real message destined for a peer
    /// simply occupies the cover slot that peer's link was going to
    /// emit anyway, so it is indistinguishable on that link from
    /// pure cover. This is the natural choice for point-to-point
    /// shaped links (e.g. a private RPC channel) and for any
    /// unicast-heavy workload.
    ///
    /// Because one shared ticker is multiplexed across peers, the
    /// per-link interval is the configured interval times the number
    /// of connected peers; a peer's effective rate therefore drops
    /// as more peers connect (and rises as they leave). An observer
    /// of a single link sees that link's constant rate change only
    /// with the node's *connection count* — never with application
    /// activity. A unicast submitted for a peer waits for that peer's
    /// next slot (bounded by `peers * interval`), occupying it in
    /// place of cover.
    ///
    /// `broadcast_shaped` in this mode fans a copy of the frame into
    /// every currently-connected peer's lane at submit time, so the
    /// message rides out on each peer's own shaped stream; peers that
    /// connect afterwards do not receive that particular frame, and
    /// per-link delivery is best-effort (a saturated link evicts its
    /// oldest queued frame rather than blocking the others). `fanout`
    /// is irrelevant in this mode.
    PerConnection {
        /// Whether to mix the round-robin cursor with a
        /// per-pick random offset (so two adjacent nodes
        /// don't always pick the same "next" peer in
        /// lockstep). Recommended: `true`.
        randomize: bool,
    },
}

/// Which priority lane to enqueue a frame into.
///
/// `peashape` exposes two priority lanes with different
/// disciplines; the scheduler always drains the high lane
/// before the low lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Lane {
    /// FIFO, bounded by [`ShapeConfig::high_lane_capacity`].
    /// Drained first by the scheduler. The natural choice for
    /// application-submitted traffic.
    High,

    /// LIFO with drop-oldest eviction, bounded by
    /// [`ShapeConfig::low_lane_capacity`]. Drained after the
    /// high lane. The natural choice for relay traffic: a
    /// freshly-enqueued frame is popped on the very next
    /// shaping tick.
    ///
    /// [`ShapeConfig::low_lane_capacity`]:
    /// crate::ShapeConfig::low_lane_capacity
    Low,
}
