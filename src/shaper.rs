//! The shared, clone-able state that backs every [`Node`].
//!
//! [`Node`]: crate::Node

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use bytes::BytesMut;
use parking_lot::Mutex;
use tokio::sync::broadcast;

use crate::config::{CoverGenerator, Lane, ShapeConfig, ShapingScope};
use crate::error::Error;
use crate::frame::{ID_SIZE, random_cover};

/// Capacity of the broadcast channel used to deliver received
/// messages to application subscribers. When the channel is full,
/// the oldest message is dropped and receivers observe
/// `RecvError::Lagged`.
///
/// Exposed as a `pub` constant so that layered protocols (e.g.
/// `peasub`) can size their *own* broadcast channels
/// consistently.
pub const SUBSCRIBER_CAPACITY: usize = 1024;

/// The on-the-wire delivery mode of a queued frame.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Target {
    /// Send to a specific peer. If the peer is not connected at
    /// tick time, the frame is silently dropped (with a debug
    /// log line).
    Unicast(SocketAddr),
    /// Send to `fanout` randomly-chosen connected peers (in
    /// [`ShapingScope::Global`]) or to a single round-robin
    /// peer (in [`ShapingScope::PerConnection`]).
    ///
    /// [`ShapingScope::Global`]: crate::ShapingScope::Global
    /// [`ShapingScope::PerConnection`]: crate::ShapingScope::PerConnection
    Broadcast,
}

/// A frame that has been queued for transmission, together with
/// its intended delivery target.
#[derive(Clone, Debug)]
pub struct PendingFrame {
    /// The frame bytes (already padded to `frame_size`).
    pub bytes: BytesMut,
    /// Where the frame should be sent.
    pub target: Target,
}

/// The pair of priority lanes maintained for a single peer in
/// [`ShapingScope::PerConnection`] mode. Same disciplines as the
/// global lanes: `high` is a bounded FIFO, `low` is a LIFO with
/// drop-oldest eviction.
#[derive(Default)]
struct PeerLanes {
    high: VecDeque<PendingFrame>,
    low: VecDeque<PendingFrame>,
}

/// The shared, node-wide state that backs every clone of a
/// [`Node`].
///
/// Two priority lanes are maintained:
///
/// - `high_lane` is a FIFO that the scheduler always drains
///   *first*. The natural choice for application-submitted
///   traffic: a fresh enqueue is sent on the very next
///   shaping tick regardless of how much other traffic has
///   piled up. Bounded; [`Error::LaneFull`] is returned once
///   it saturates.
/// - `low_lane` is a LIFO with drop-oldest eviction. The
///   natural choice for relay traffic: a freshly-enqueued
///   frame is pushed to the front and the next tick pops it
///   without waiting in line behind older relays. Under
///   sustained inflow, the oldest queued frame is discarded
///   first to make room.
///
/// In addition, a `tokio::sync::broadcast` channel is used to
/// deliver every frame that arrives from a peer to all
/// application subscribers — both real and cover traffic pass
/// through the same channel, because on the wire they are
/// indistinguishable.
///
/// # Per-connection lanes
///
/// In [`ShapingScope::PerConnection`] mode the global `high_lane`
/// and `low_lane` are *not* used. Instead each peer gets its own
/// pair of lanes (`peer_lanes`), drained on that peer's scheduled
/// slot, so a real frame for a peer simply occupies the cover slot
/// that peer's link was going to emit anyway. `pc_peers` caches the
/// most recent connected-peer set (refreshed by the scheduler each
/// tick) so that a `Broadcast` enqueue can fan a copy into every
/// peer's lane without the shaper needing direct access to the
/// `pea2pea` node.
///
/// [`Node`]: crate::Node
/// [`Error::LaneFull`]: crate::Error::LaneFull
pub struct Shaper {
    config: ShapeConfig,
    incoming: broadcast::Sender<BytesMut>,
    high_lane: Mutex<VecDeque<PendingFrame>>,
    low_lane: Mutex<VecDeque<PendingFrame>>,
    /// Per-peer lanes, used only in [`ShapingScope::PerConnection`].
    peer_lanes: Mutex<HashMap<SocketAddr, PeerLanes>>,
    /// Most-recent connected-peer snapshot, used only in
    /// [`ShapingScope::PerConnection`] to fan out `Broadcast`
    /// enqueues. Refreshed by the scheduler on every tick.
    pc_peers: Mutex<Vec<SocketAddr>>,
    shutting_down: AtomicBool,
    /// The generator consulted to build a cover frame when a tick
    /// fires with empty lanes. Resolved from
    /// [`ShapeConfig::cover_generator`], defaulting to
    /// [`random_cover`].
    cover_generator: Arc<dyn CoverGenerator>,
}

impl Shaper {
    /// Builds a fresh `Shaper` from the given configuration.
    ///
    /// # Panics
    ///
    /// Panics if `config.high_lane_capacity` or
    /// `config.low_lane_capacity` is `0` (no buffering is
    /// possible), or if `config.frame_size < ID_SIZE` (a frame
    /// could not hold its identifier prefix; the framing helpers
    /// would underflow).
    pub fn new(config: &ShapeConfig) -> Self {
        assert!(
            config.high_lane_capacity > 0,
            "high_lane_capacity must be non-zero",
        );
        assert!(
            config.low_lane_capacity > 0,
            "low_lane_capacity must be non-zero",
        );
        assert!(
            config.frame_size >= ID_SIZE,
            "frame_size ({} bytes) must be at least ID_SIZE ({ID_SIZE} bytes)",
            config.frame_size,
        );
        let (incoming, _) = broadcast::channel(SUBSCRIBER_CAPACITY);
        // Resolve the cover generator now: a caller-supplied one, or
        // the built-in uniform-random default.
        let cover_generator: Arc<dyn CoverGenerator> = match &config.cover_generator {
            Some(g) => g.clone(),
            None => Arc::new(random_cover as fn(&ShapeConfig) -> BytesMut),
        };
        Self {
            config: config.clone(),
            incoming,
            high_lane: Mutex::new(VecDeque::with_capacity(config.high_lane_capacity)),
            low_lane: Mutex::new(VecDeque::with_capacity(config.low_lane_capacity)),
            peer_lanes: Mutex::new(HashMap::new()),
            pc_peers: Mutex::new(Vec::new()),
            shutting_down: AtomicBool::new(false),
            cover_generator,
        }
    }

    /// Returns a reference to the configuration this shaper was
    /// built from.
    pub fn config(&self) -> &ShapeConfig {
        &self.config
    }

    /// Returns a clone of the broadcast sender used to deliver
    /// received messages to application subscribers.
    pub fn incoming(&self) -> broadcast::Sender<BytesMut> {
        self.incoming.clone()
    }

    /// Returns a shared reference to the shutdown flag. The
    /// scheduler polls this on every iteration of its loop.
    pub fn shutting_down(&self) -> &AtomicBool {
        &self.shutting_down
    }

    /// Enqueue a real (application-originated) message into the
    /// named lane.
    ///
    /// The payload is padded to `frame_size` with random bytes;
    /// on the wire it is therefore indistinguishable from a
    /// cover message. The returned identifier is the random
    /// 32-byte ID that the message has been assigned; the
    /// application can use it (e.g. correlated with its own
    /// bookkeeping) but it has no on-the-wire significance
    /// beyond dedup at intermediate nodes.
    ///
    /// The high-priority lane is FIFO; once it saturates, further
    /// enqueues return [`Error::LaneFull`]. The low-priority
    /// lane is LIFO with drop-oldest: a fresh frame is always
    /// accepted, but if the lane is at capacity, the oldest
    /// frame is silently evicted from the back to make room.
    ///
    /// # Errors
    ///
    /// - [`Error::PayloadTooLarge`] if `payload.len()` exceeds
    ///   `frame_size - ID_SIZE`.
    /// - [`Error::LaneFull`] if the high-priority lane is at
    ///   capacity (only possible with `lane == Lane::High`).
    pub fn enqueue(
        &self,
        lane: Lane,
        target: Target,
        payload: &[u8],
    ) -> Result<[u8; ID_SIZE], Error> {
        let max_payload = self.config.frame_size - ID_SIZE;
        if payload.len() > max_payload {
            return Err(Error::PayloadTooLarge {
                size: payload.len(),
                max: max_payload,
            });
        }

        let (id, msg) = crate::frame::build_frame(&self.config, payload);
        self.enqueue_raw(lane, target, msg)?;
        Ok(id)
    }

    /// Enqueue a *pre-built* frame into the named lane.
    ///
    /// Unlike [`Shaper::enqueue`], this method does not pad or
    /// generate an ID; the caller is responsible for providing
    /// a fully-shaped frame of exactly `frame_size` bytes.
    ///
    /// This is the right method for protocols that need to
    /// re-broadcast a frame received from a peer *byte-for-byte
    /// unchanged* (so that an intermediate node's dedup cache
    /// can recognize the same ID), or for protocols that want
    /// to use a different frame-construction convention than
    /// peashape's defaults.
    ///
    /// # Errors
    ///
    /// - [`Error::FrameSizeMismatch`] if `frame.len() !=
    ///   frame_size`.
    /// - [`Error::LaneFull`] if the high-priority lane is at
    ///   capacity (only possible with `lane == Lane::High` and a
    ///   `Unicast`/global enqueue; `Broadcast` fan-out in
    ///   `PerConnection` mode is best-effort and never returns
    ///   `LaneFull`).
    pub fn enqueue_raw(&self, lane: Lane, target: Target, frame: BytesMut) -> Result<(), Error> {
        if frame.len() != self.config.frame_size {
            return Err(Error::FrameSizeMismatch {
                size: frame.len(),
                expected: self.config.frame_size,
            });
        }
        match self.config.scope {
            // Global scope: one shared pair of lanes; the scheduler
            // decides fanout/recipients at dispatch time.
            ShapingScope::Global => self.push_global(
                lane,
                PendingFrame {
                    bytes: frame,
                    target,
                },
            ),
            // Per-connection scope: route the frame to the lane(s) of
            // the peer(s) it is destined for. The scheduler drains
            // each peer's lane on that peer's own scheduled slot.
            ShapingScope::PerConnection { .. } => match target {
                Target::Unicast(peer) => self.push_peer(peer, lane, frame),
                Target::Broadcast => {
                    self.fan_out_per_connection(lane, frame);
                    Ok(())
                }
            },
        }
    }

    /// Push a frame onto the shared (global-scope) lanes.
    fn push_global(&self, lane: Lane, pframe: PendingFrame) -> Result<(), Error> {
        match lane {
            Lane::High => {
                let mut l = self.high_lane.lock();
                if l.len() >= self.config.high_lane_capacity {
                    return Err(Error::LaneFull);
                }
                l.push_back(pframe);
            }
            Lane::Low => {
                let mut l = self.low_lane.lock();
                while l.len() >= self.config.low_lane_capacity {
                    l.pop_back();
                }
                l.push_front(pframe);
            }
        }
        Ok(())
    }

    /// Push a unicast frame onto a single peer's per-connection
    /// lane (PerConnection scope). High is bounded FIFO
    /// ([`Error::LaneFull`] when full); Low is LIFO with
    /// drop-oldest.
    fn push_peer(&self, peer: SocketAddr, lane: Lane, frame: BytesMut) -> Result<(), Error> {
        let pframe = PendingFrame {
            bytes: frame,
            target: Target::Unicast(peer),
        };
        let mut map = self.peer_lanes.lock();
        let lanes = map.entry(peer).or_default();
        match lane {
            Lane::High => {
                if lanes.high.len() >= self.config.high_lane_capacity {
                    return Err(Error::LaneFull);
                }
                lanes.high.push_back(pframe);
            }
            Lane::Low => {
                while lanes.low.len() >= self.config.low_lane_capacity {
                    lanes.low.pop_back();
                }
                lanes.low.push_front(pframe);
            }
        }
        Ok(())
    }

    /// Fan a broadcast frame into every cached peer's per-connection
    /// lane (PerConnection scope). Best-effort: a peer whose lane is
    /// full has its oldest queued frame evicted to make room rather
    /// than failing the whole broadcast, so no recipient can stall
    /// delivery to the others. Peers that connect *after* this call
    /// will not receive this particular frame.
    fn fan_out_per_connection(&self, lane: Lane, frame: BytesMut) {
        let peers = self.pc_peers.lock().clone();
        if peers.is_empty() {
            return;
        }
        let mut map = self.peer_lanes.lock();
        for peer in peers {
            let pframe = PendingFrame {
                bytes: frame.clone(),
                target: Target::Unicast(peer),
            };
            let lanes = map.entry(peer).or_default();
            match lane {
                Lane::High => {
                    while lanes.high.len() >= self.config.high_lane_capacity {
                        lanes.high.pop_front();
                    }
                    lanes.high.push_back(pframe);
                }
                Lane::Low => {
                    while lanes.low.len() >= self.config.low_lane_capacity {
                        lanes.low.pop_back();
                    }
                    lanes.low.push_front(pframe);
                }
            }
        }
    }

    /// Returns the next frame to put on the wire, *or* `None` if
    /// both lanes are empty and the caller should generate
    /// cover.
    ///
    /// The high-priority lane is drained first; only when it is
    /// empty is the low-priority lane consulted.
    pub fn next_frame(&self) -> Option<PendingFrame> {
        if let Some(f) = self.high_lane.lock().pop_front() {
            return Some(f);
        }
        self.low_lane.lock().pop_front()
    }

    /// Returns the next frame to put on the wire for `peer`
    /// (PerConnection scope), draining that peer's own high lane
    /// first and then its low lane, or `None` if the peer has no
    /// queued frames and the caller should generate cover.
    pub fn next_frame_for(&self, peer: SocketAddr) -> Option<PendingFrame> {
        let mut map = self.peer_lanes.lock();
        let lanes = map.get_mut(&peer)?;
        if let Some(f) = lanes.high.pop_front() {
            return Some(f);
        }
        lanes.low.pop_front()
    }

    /// Refreshes the cached connected-peer set consulted by
    /// `Broadcast` fan-out in [`ShapingScope::PerConnection`] mode.
    /// Called by the scheduler on every tick.
    pub fn refresh_pc_peers(&self, peers: &[SocketAddr]) {
        let mut cache = self.pc_peers.lock();
        cache.clear();
        cache.extend_from_slice(peers);
    }

    /// Drops the per-connection lanes of any peer not in
    /// `connected`, so queued frames for a departed peer are
    /// discarded (mirroring the global-scope behavior, where a
    /// unicast to a disconnected peer is dropped) and the map does
    /// not grow without bound. Called by the scheduler on every
    /// tick in [`ShapingScope::PerConnection`] mode.
    pub fn prune_peer_lanes(&self, connected: &[SocketAddr]) {
        let mut map = self.peer_lanes.lock();
        if map.is_empty() {
            return;
        }
        let keep: HashSet<SocketAddr> = connected.iter().copied().collect();
        map.retain(|addr, _| keep.contains(addr));
    }

    /// Returns the total number of frames currently queued, across
    /// both the global lanes (Global scope) and all per-connection
    /// lanes (PerConnection scope). Only one of the two sets is
    /// populated for a given node, so this is simply their sum.
    pub fn queued(&self) -> usize {
        let global = self.high_lane.lock().len() + self.low_lane.lock().len();
        let per_peer: usize = self
            .peer_lanes
            .lock()
            .values()
            .map(|l| l.high.len() + l.low.len())
            .sum();
        global + per_peer
    }

    /// Returns a freshly-generated cover frame, identical in shape to
    /// a real one. Delegates to the configured [`CoverGenerator`]
    /// (the uniform-random [`random_cover`] unless a custom one was
    /// supplied via [`ShapeConfig::cover_generator`]).
    pub fn cover(&self) -> PendingFrame {
        let bytes = self.cover_generator.cover(&self.config);
        debug_assert_eq!(
            bytes.len(),
            self.config.frame_size,
            "cover generator returned a frame of the wrong size",
        );
        PendingFrame {
            bytes,
            target: Target::Broadcast,
        }
    }

    /// Runs a real (lane-drained) frame through the configured
    /// [`CoverGenerator`]'s
    /// [`finalize_real`](CoverGenerator::finalize_real) hook, in send
    /// order, preserving its delivery target. With the default
    /// generator this is a no-op; a custom generator can use it to
    /// sequence real frames into the same stream as cover.
    pub fn finalize_real(&self, mut frame: PendingFrame) -> PendingFrame {
        frame.bytes = self
            .cover_generator
            .finalize_real(&self.config, frame.bytes);
        debug_assert_eq!(
            frame.bytes.len(),
            self.config.frame_size,
            "cover generator finalize_real returned a frame of the wrong size",
        );
        frame
    }

    /// Dispatches a frame received from a peer to all
    /// application subscribers via the broadcast channel.
    ///
    /// Silently drops frames of the wrong size (a misbehaving or
    /// non-`peashape` peer); well-formed frames are fanned out
    /// to every active subscriber. Late subscribers see the
    /// frame only if the broadcast channel hasn't already
    /// recycled its slot.
    pub fn handle_incoming(&self, message: BytesMut) {
        if message.len() != self.config.frame_size {
            return;
        }
        let _ = self.incoming.send(message);
    }
}
