//! The background task that turns a [`Node`] into a
//! metadata-private one.
//!
//! [`Node`]: crate::Node

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::Notify;
use tokio::time;
use tracing::debug;

use crate::config::{ShapingScope, ShapingStrategy};
use crate::node::Node;
use crate::shaper::{PendingFrame, Shaper, Target};
use pea2pea::{protocols::Writing, Pea2Pea};

/// The background task that drains a [`Shaper`] at the configured
/// rate, picking one frame per tick (real if any are queued, cover
/// otherwise) and shipping it to the right peer(s).
///
/// [`Shaper`]: crate::Shaper
pub struct Scheduler {
    shaper: Arc<Shaper>,
    strategy: ShapingStrategy,
    /// Used to break the scheduler out of its long `Poisson` sleeps
    /// promptly when [`Node::shutdown`] is called.
    ///
    /// [`Node::shutdown`]: crate::Node::shutdown
    wake: Notify,
    /// Per-connection round-robin cursor (used only by
    /// [`ShapingScope::PerConnection`]). The cursor is mixed with
    /// a per-pick random offset to avoid the pathological case of
    /// every node in a small ring happening to select the same
    /// peer on the same tick.
    cursor: Mutex<usize>,
}

impl Scheduler {
    /// Creates a new scheduler that drains `shaper` according to
    /// `strategy`.
    pub fn new(shaper: Arc<Shaper>, strategy: ShapingStrategy) -> Self {
        Self {
            shaper,
            strategy,
            wake: Notify::new(),
            cursor: Mutex::new(0),
        }
    }

    /// Wakes the scheduler. Called by [`Node::shutdown`] so a
    /// `Poisson` task that is currently sleeping for a long
    /// interval can be torn down promptly.
    ///
    /// Uses [`Notify::notify_one`] (not `notify_waiters`) so the
    /// notification is preserved if the scheduler is between
    /// `notified().await` calls when shutdown is requested —
    /// the next iteration of the scheduler loop will then
    /// observe the wake and exit.
    ///
    /// [`Node::shutdown`]: crate::Node::shutdown
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// Drives the shaping scheduler for the lifetime of the
    /// node. Returns when the node is shutting down.
    pub async fn run(&self, node: Node) {
        match self.strategy {
            ShapingStrategy::Constant { interval } => self.run_constant(node, interval).await,
            ShapingStrategy::Poisson { rate } => self.run_poisson(node, rate).await,
        }
    }

    async fn run_constant(&self, node: Node, interval: Duration) {
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        // Consume the immediate first tick that `time::interval`
        // would otherwise fire at `t=0`; we don't want to send a
        // message immediately on startup, only after the first
        // full interval has elapsed.
        ticker.tick().await;

        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = self.wake.notified() => break,
            }
            if self.is_shutting_down() {
                break;
            }
            self.send_one(&node).await;
        }
    }

    async fn run_poisson(&self, node: Node, rate: f64) {
        loop {
            // Compute the next inter-arrival *before* the await
            // point so that the (non-`Send`) thread-local RNG does
            // not have to be held across it.
            let dur = {
                let mut rng = rand::rng();
                // Inter-arrival time for a Poisson process of rate
                // `rate` is Exp(rate). Sample via inverse-CDF:
                // -ln(U) / rate, where U is uniform in (0, 1).
                // We clamp the upper end so a single
                // astronomically-large draw cannot stall shutdown.
                let u: f64 = rng.random::<f64>().clamp(f64::MIN_POSITIVE, 1.0);
                let secs = (-u.ln() / rate).min(60.0);
                Duration::from_secs_f64(secs)
            };

            tokio::select! {
                _ = time::sleep(dur) => {}
                _ = self.wake.notified() => break,
            }
            if self.is_shutting_down() {
                break;
            }
            self.send_one(&node).await;
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shaper.shutting_down().load(Ordering::SeqCst)
    }

    /// Pull the next frame (real or cover) and ship it according
    /// to the configured [`ShapingScope`].
    async fn send_one(&self, node: &Node) {
        let peers = node.connected_peers();
        if peers.is_empty() {
            return;
        }

        let frame = self
            .shaper
            .next_frame()
            .unwrap_or_else(|| self.shaper.cover());
        self.dispatch(node, &peers, frame);
    }

    /// Dispatch a frame to its destination(s).
    ///
    /// Honors a `Target::Unicast` first: if the target peer is
    /// connected, the frame is sent there. If the target peer
    /// has disconnected between submission and tick time, the
    /// frame falls through to the scope-based dispatch (so the
    /// shaping rate is still maintained — the user expected a
    /// frame to go out on this tick, and a best-effort
    /// broadcast is preferable to silently dropping user data).
    fn dispatch(&self, node: &Node, peers: &[SocketAddr], frame: PendingFrame) {
        if let Target::Unicast(peer) = &frame.target {
            if peers.contains(peer) {
                self.send_one_to(node, *peer, &frame);
                return;
            }
            // The target peer disconnected between submission
            // and the tick that drains the lane. Fall through
            // to scope-based dispatch (best-effort).
            debug!(
                parent: node.node().span(),
                "unicast target {peer} is no longer connected; falling back to scope-based dispatch"
            );
        }
        match self.shaper.config().scope {
            ShapingScope::Global => {
                let fanout = self.shaper.config().fanout.min(peers.len()).max(1);
                for peer in self.pick_targets(peers, fanout) {
                    self.send_one_to(node, peer, &frame);
                }
            }
            ShapingScope::PerConnection { randomize } => {
                let pick = self.pick_round_robin(peers, randomize);
                self.send_one_to(node, pick, &frame);
            }
        }
    }

    fn send_one_to(&self, node: &Node, peer: SocketAddr, frame: &PendingFrame) {
        if let Err(e) = node.unicast_fast(peer, frame.bytes.clone()) {
            debug!(parent: node.node().span(), "send to {peer} failed: {e}");
        }
    }

    /// Pick `fanout` distinct peer addresses from `peers` using
    /// a cursor + per-pick random offset. The cursor advances on
    /// every pick, so over time the selection rotates through
    /// the whole peer set even when `fanout` is small.
    fn pick_targets(&self, peers: &[SocketAddr], fanout: usize) -> Vec<SocketAddr> {
        let mut rng = rand::rng();
        let mut cursor = self.cursor.lock();
        let n = peers.len();
        if n == 0 {
            return Vec::new();
        }
        // Work on a list of *positions* so removal is O(n) but
        // bounded by the peer count.
        let mut pool: Vec<usize> = (0..n).collect();
        let mut chosen: Vec<SocketAddr> = Vec::with_capacity(fanout);
        for _ in 0..fanout.min(n) {
            let remaining = pool.len();
            if remaining == 0 {
                break;
            }
            let offset = rng.random_range(0..remaining);
            let pick = (*cursor + offset) % remaining;
            *cursor = cursor.wrapping_add(1);
            let pos = pool.remove(pick);
            chosen.push(peers[pos]);
        }
        chosen
    }

    /// Pick a single peer via the round-robin cursor (mixed with
    /// a per-pick random offset when `randomize` is `true`).
    fn pick_round_robin(&self, peers: &[SocketAddr], randomize: bool) -> SocketAddr {
        let n = peers.len();
        debug_assert!(n > 0);
        let mut cursor = self.cursor.lock();
        let pick = if randomize {
            let mut rng = rand::rng();
            let offset = rng.random_range(0..n);
            (*cursor + offset) % n
        } else {
            *cursor % n
        };
        *cursor = cursor.wrapping_add(1);
        peers[pick]
    }
}
