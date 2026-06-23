//! The shared, clone-able state that backs every [`Node`].
//!
//! [`Node`]: crate::Node

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;

use bytes::BytesMut;
use parking_lot::Mutex;
use tokio::sync::broadcast;

use crate::config::{Lane, ShapeConfig};
use crate::error::Error;
use crate::frame::{random_cover, ID_SIZE};

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
/// [`Node`]: crate::Node
/// [`Error::LaneFull`]: crate::Error::LaneFull
pub struct Shaper {
    config: ShapeConfig,
    incoming: broadcast::Sender<BytesMut>,
    high_lane: Mutex<VecDeque<PendingFrame>>,
    low_lane: Mutex<VecDeque<PendingFrame>>,
    shutting_down: AtomicBool,
}

impl Shaper {
    /// Builds a fresh `Shaper` from the given configuration.
    ///
    /// # Panics
    ///
    /// Panics if `config.high_lane_capacity` or
    /// `config.low_lane_capacity` is `0` (no buffering is
    /// possible).
    pub fn new(config: &ShapeConfig) -> Self {
        assert!(
            config.high_lane_capacity > 0,
            "high_lane_capacity must be non-zero",
        );
        assert!(
            config.low_lane_capacity > 0,
            "low_lane_capacity must be non-zero",
        );
        let (incoming, _) = broadcast::channel(SUBSCRIBER_CAPACITY);
        Self {
            config: config.clone(),
            incoming,
            high_lane: Mutex::new(VecDeque::with_capacity(config.high_lane_capacity)),
            low_lane: Mutex::new(VecDeque::with_capacity(config.low_lane_capacity)),
            shutting_down: AtomicBool::new(false),
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
    ///   capacity (only possible with `lane == Lane::High`).
    pub fn enqueue_raw(&self, lane: Lane, target: Target, frame: BytesMut) -> Result<(), Error> {
        if frame.len() != self.config.frame_size {
            return Err(Error::FrameSizeMismatch {
                size: frame.len(),
                expected: self.config.frame_size,
            });
        }
        let pframe = PendingFrame {
            bytes: frame,
            target,
        };
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

    /// Returns the total number of frames currently queued in
    /// both lanes.
    pub fn queued(&self) -> usize {
        self.high_lane.lock().len() + self.low_lane.lock().len()
    }

    /// Returns a freshly-generated cover frame, identical in
    /// shape to a real one.
    pub fn cover(&self) -> PendingFrame {
        PendingFrame {
            bytes: random_cover(&self.config),
            target: Target::Broadcast,
        }
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
