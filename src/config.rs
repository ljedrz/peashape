//! Configuration types for a [`Node`].
//!
//! [`Node`]: crate::Node

use std::time::Duration;

/// The set of parameters that govern a [`Node`].
///
/// Most callers only need to set [`ShapeConfig::strategy`]; every
/// other field has a sensible default. To see what the defaults
/// are, use [`ShapeConfig::default`] and read the resulting
/// struct.
///
/// [`Node`]: crate::Node
#[derive(Clone, Debug)]
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
        }
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

    /// One ticker per connection. Every tick, one frame is sent to
    /// a single peer, with peers selected round-robin (optionally
    /// with a per-tick random offset to avoid two adjacent nodes
    /// choosing the same "next" peer in lockstep).
    ///
    /// `fanout` is irrelevant in this mode (clamped to 1).
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
