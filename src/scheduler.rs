//! The background task that turns a [`Node`] into a
//! metadata-private one.
//!
//! [`Node`]: crate::Node

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::Notify;
use tokio::time;
use tracing::debug;

use crate::config::{ShapingScope, ShapingStrategy};
use crate::node::Node;
use crate::shaper::{PendingFrame, Shaper, Target};
use pea2pea::{Pea2Pea, protocols::Writing};

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
    /// The peers selected on the previous [`ShapingScope::Global`]
    /// tick, used to bias the next selection toward *fresh* peers.
    /// This prevents the gossip "ping-pong" failure mode in which two
    /// adjacent nodes bounce a frame back and forth without it
    /// reaching the rest of the overlay. Unused in
    /// [`ShapingScope::PerConnection`] mode.
    last_peers: Mutex<Vec<SocketAddr>>,
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
            last_peers: Mutex::new(Vec::new()),
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
                //
                // We bound a single pathological draw (U very close
                // to 0 yields an enormous interval, which could push
                // `from_secs_f64` toward overflow). Shutdown latency
                // is *not* a concern here — the `select!` below wakes
                // the task immediately via `self.wake`, regardless of
                // how long the sleep was scheduled for.
                //
                // The bound is scaled to the configured rate
                // (`MAX_INTERVAL_FACTOR / rate`, i.e. a fixed multiple
                // of the mean inter-arrival) rather than a flat number
                // of seconds. A flat cap would silently truncate most
                // of the distribution at low rates (e.g. a 60 s cap on
                // a rate of 0.01/s — mean 100 s — discards the bulk of
                // the mass and the emitted process is no longer
                // Poisson). Scaling keeps the truncated tail mass at a
                // negligible `exp(-MAX_INTERVAL_FACTOR)` for every
                // rate, so the observed process is Poisson to within
                // that vanishingly small tail.
                const MAX_INTERVAL_FACTOR: f64 = 40.0; // exp(-40) ~ 4e-18
                let u: f64 = rng.random::<f64>().clamp(f64::MIN_POSITIVE, 1.0);
                let secs = (-u.ln() / rate).min(MAX_INTERVAL_FACTOR / rate);
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

    /// Pull and ship the frame(s) for one tick, according to the
    /// configured [`ShapingScope`].
    ///
    /// - [`ShapingScope::Global`]: drain the single shared lane and
    ///   fan the frame out (see [`Scheduler::dispatch_global`]).
    /// - [`ShapingScope::PerConnection`]: select the peer whose turn
    ///   it is and send exactly one frame *to that peer* — a real
    ///   frame from that peer's own lane if one is waiting, otherwise
    ///   cover. Because the scheduled *peer* (not the queued frame)
    ///   drives selection, a real frame for that peer simply occupies
    ///   the cover slot its link was going to emit anyway, so it is
    ///   indistinguishable on that link from pure cover. There is no
    ///   off-schedule transmission and no peer is ever skipped.
    async fn send_one(&self, node: &Node) {
        let peers = node.connected_peers();
        match self.shaper.config().scope {
            ShapingScope::Global => {
                if peers.is_empty() {
                    return;
                }
                // A real frame is finalized through the cover generator
                // (in send order) so a custom generator can sequence it
                // into the same stream as cover; an empty lane yields a
                // fresh cover frame.
                let frame = match self.shaper.next_frame() {
                    Some(real) => self.shaper.finalize_real(real),
                    None => self.shaper.cover(),
                };
                self.dispatch_global(node, &peers, frame);
            }
            ShapingScope::PerConnection { randomize } => {
                // Keep the per-connection bookkeeping in step with the
                // live connection set: refresh the cache that broadcast
                // fan-out consults, and discard lanes for departed peers.
                self.shaper.refresh_pc_peers(&peers);
                self.shaper.prune_peer_lanes(&peers);
                if peers.is_empty() {
                    return;
                }
                let peer = self.pick_round_robin(&peers, randomize);
                // Finalize a real frame (in send order) through the
                // cover generator so it shares the cover stream's
                // sequencing; otherwise emit a fresh cover frame.
                let frame = match self.shaper.next_frame_for(peer) {
                    Some(real) => self.shaper.finalize_real(real),
                    None => self.shaper.cover(),
                };
                self.send_one_to(node, peer, &frame);
            }
        }
    }

    /// Dispatch a [`ShapingScope::Global`] frame to its
    /// destination(s).
    ///
    /// The cardinal rule is that the *number* of frames a tick
    /// puts on the wire must not depend on whether the drained
    /// frame is real or cover, nor on whether it is unicast or
    /// broadcast — otherwise a passive observer counting frames
    /// per tick could pick out the ticks that carried real
    /// unicast traffic (and their recipient). So every tick emits
    /// exactly `fanout` frames: for a [`Target::Broadcast`] frame
    /// those are `fanout` copies of the (real or cover) frame; for
    /// a [`Target::Unicast`] frame, the real frame goes to its
    /// recipient and the remaining `fanout - 1` slots are filled
    /// with freshly-generated cover to *other* peers.
    ///
    /// If a unicast target has disconnected between submission and
    /// tick time, the real frame is dropped — it must never be
    /// delivered to a peer it was not addressed to — and the tick
    /// emits `fanout` freshly-generated cover frames instead, so the
    /// on-the-wire frame count for the tick is unchanged.
    ///
    /// Note: under `Global` scope a *residual* aggregate signal
    /// remains — a peer that is the recipient of sustained unicast
    /// traffic receives frames at a marginally higher long-run rate
    /// than its cover-only share. This is inherent to carrying
    /// unicast over a gossip-style fanout; [`ShapingScope::PerConnection`]
    /// avoids it entirely (a real frame merely replaces a cover
    /// frame on the recipient's already-constant stream), and is
    /// the recommended scope for unicast-heavy workloads.
    fn dispatch_global(&self, node: &Node, peers: &[SocketAddr], frame: PendingFrame) {
        let fanout = self.shaper.config().fanout.min(peers.len()).max(1);
        match &frame.target {
            // Unicast to a still-connected peer: real frame to the
            // recipient, cover to the remaining slots so the tick still
            // emits exactly `fanout` frames.
            Target::Unicast(peer) if peers.contains(peer) => {
                let target = *peer;
                self.send_one_to(node, target, &frame);
                let others: Vec<SocketAddr> =
                    peers.iter().copied().filter(|p| *p != target).collect();
                let cover_n = fanout.saturating_sub(1).min(others.len());
                for peer in self.pick_targets(&others, cover_n) {
                    self.send_one_to(node, peer, &self.shaper.cover());
                }
            }
            // Unicast whose target has disconnected between submission
            // and tick time: drop the real payload (it must never reach
            // a peer it was not addressed to) and emit `fanout` cover
            // frames so the tick's on-the-wire frame count is unchanged.
            Target::Unicast(peer) => {
                debug!(
                    parent: node.node().span(),
                    "unicast target {peer} is no longer connected; dropping the frame and emitting cover"
                );
                for peer in self.pick_targets(peers, fanout) {
                    self.send_one_to(node, peer, &self.shaper.cover());
                }
            }
            // Broadcast: the (real or cover) frame goes to `fanout` peers.
            Target::Broadcast => {
                for peer in self.pick_targets(peers, fanout) {
                    self.send_one_to(node, peer, &frame);
                }
            }
        }
    }

    fn send_one_to(&self, node: &Node, peer: SocketAddr, frame: &PendingFrame) {
        if let Err(e) = node.unicast_fast(peer, frame.bytes.clone()) {
            debug!(parent: node.node().span(), "send to {peer} failed: {e}");
        }
    }

    /// Pick `fanout` distinct peer addresses from `peers`, biasing the
    /// selection away from the peers chosen on the previous tick.
    ///
    /// The selection proceeds in two phases:
    ///
    /// 1. Prefer "fresh" peers (not in `last_peers`). This avoids the
    ///    gossip ping-pong failure mode where two adjacent nodes bounce
    ///    a frame between themselves instead of spreading it.
    /// 2. If the fresh pool is exhausted before `fanout` peers are
    ///    chosen, fall back to the recently-used pool.
    ///
    /// Within each pool a cursor + per-pick random offset rotates the
    /// selection through the whole peer set over time and keeps two
    /// adjacent nodes from selecting the same "next" peer in lockstep.
    fn pick_targets(&self, peers: &[SocketAddr], fanout: usize) -> Vec<SocketAddr> {
        let n = peers.len();
        if n == 0 {
            return Vec::new();
        }
        let mut rng = rand::rng();
        let mut cursor = self.cursor.lock();
        let mut last_peers = self.last_peers.lock();

        // Partition peer *positions* into fresh (not sent to last tick)
        // and recently-used pools.
        let mut fresh: Vec<usize> = Vec::new();
        let mut used: Vec<usize> = Vec::new();
        for (i, p) in peers.iter().enumerate() {
            if last_peers.contains(p) {
                used.push(i);
            } else {
                fresh.push(i);
            }
        }

        let mut chosen: Vec<SocketAddr> = Vec::with_capacity(fanout.min(n));
        for _ in 0..fanout.min(n) {
            let pool = if !fresh.is_empty() {
                &mut fresh
            } else if !used.is_empty() {
                &mut used
            } else {
                break;
            };
            let remaining = pool.len();
            let offset = rng.random_range(0..remaining);
            let pick = (*cursor + offset) % remaining;
            *cursor = cursor.wrapping_add(1);
            chosen.push(peers[pool.remove(pick)]);
        }

        // Remember this tick's selection to bias the next one.
        last_peers.clear();
        last_peers.extend_from_slice(&chosen);
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
