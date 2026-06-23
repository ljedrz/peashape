//! The [`Node`] type and its `pea2pea` protocol implementations.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::BytesMut;
use parking_lot::Mutex;
use pea2pea::{
    protocols::{Reading, Writing},
    Config, ConnectionSide, Node as P2pNode, Pea2Pea,
};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::codec::Codec;
use crate::config::{Lane, ShapeConfig, ShapingScope};
use crate::error::Error;
use crate::frame::ID_SIZE;
use crate::scheduler::Scheduler;
use crate::shaper::{Shaper, Target};

/// A single peer in a `peashape` network.
///
/// Internally, a `Node` is a `pea2pea::Node` plus a small layer
/// of metadata-private bookkeeping: a bounded *high-priority
/// lane* (FIFO; always drained first), a bounded *low-priority
/// lane* (LIFO with drop-oldest), a broadcast channel of
/// incoming frames, and a background shaping-scheduler task
/// that drains the lanes at a constant (or Poisson-distributed)
/// rate, generating cover traffic when both lanes are empty.
///
/// `Node` is cheap to `clone`: clones share the same lanes,
/// broadcast channel, and shaping task.
#[derive(Clone)]
pub struct Node {
    p2p: P2pNode,
    shaper: Arc<Shaper>,
    scheduler: Arc<Scheduler>,
    scheduler_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Node {
    /// Constructs a new, unstarted `Node`. Call [`Node::spawn`] to
    /// enable the `pea2pea` protocols, bring up the listener, and
    /// launch the shaping scheduler.
    ///
    /// # Panics
    ///
    /// Panics if the configuration is internally inconsistent:
    ///
    /// - `frame_size == 0` (no message can be encoded);
    /// - `frame_size > max_frame_size` (would be rejected by the
    ///   decoder);
    /// - `high_lane_capacity == 0` or `low_lane_capacity == 0`
    ///   (no buffering is possible);
    /// - `fanout == 0` and `scope == Global` (no peers would ever
    ///   be selected).
    pub fn new(config: ShapeConfig) -> Self {
        let p2p_config = Config {
            name: config.name.clone(),
            listener_addr: config.listener_addr,
            max_connections: config.max_connections,
            max_connections_per_ip: config.max_connections_per_ip,
            reuse_listener_port: config.reuse_listener_port,
            ..Config::default()
        };

        assert!(config.frame_size > 0, "frame_size must be non-zero");
        assert!(
            config.frame_size <= config.max_frame_size,
            "frame_size ({} bytes) must not exceed max_frame_size ({} bytes)",
            config.frame_size,
            config.max_frame_size,
        );
        if matches!(config.scope, ShapingScope::Global) {
            assert!(
                config.fanout > 0,
                "fanout must be non-zero when scope is Global"
            );
        }

        let p2p = P2pNode::new(p2p_config);
        let shaper = Arc::new(Shaper::new(&config));
        let scheduler = Arc::new(Scheduler::new(shaper.clone(), config.strategy));
        Self {
            p2p,
            shaper,
            scheduler,
            scheduler_handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Starts the node.
    ///
    /// Enables the `pea2pea` `Reading` and `Writing` protocols,
    /// brings up the listener (if one is configured), and spawns
    /// the shaping scheduler. Returns the bound listening
    /// address, or `None` if the node was configured without a
    /// listener.
    pub async fn spawn(&self) -> io::Result<Option<SocketAddr>> {
        self.enable_reading().await;
        self.enable_writing().await;
        let addr = self.p2p.toggle_listener().await?;

        let scheduler = self.scheduler.clone();
        let node = self.clone();
        let handle = tokio::spawn(async move {
            scheduler.run(node).await;
        });
        *self.scheduler_handle.lock() = Some(handle);

        Ok(addr)
    }

    /// Submits a real (application-originated) message to be
    /// shaped and sent to a specific peer. The frame is queued
    /// on the high-priority lane.
    ///
    /// See [`Shaper::enqueue`] for the full set of payload
    /// semantics and error conditions. The high-priority lane
    /// is the natural choice for application-submitted traffic:
    /// the next shaping tick transmits a high-priority frame
    /// regardless of how much other traffic has piled up.
    ///
    /// [`Shaper::enqueue`]: crate::Shaper::enqueue
    pub fn send_shaped(&self, peer: SocketAddr, payload: &[u8]) -> Result<[u8; ID_SIZE], Error> {
        self.shaper
            .enqueue(Lane::High, Target::Unicast(peer), payload)
    }

    /// Submits a real (application-originated) message to be
    /// shaped and broadcast. The frame is queued on the
    /// high-priority lane; the next shaping tick will ship the
    /// frame to `fanout` randomly-chosen connected peers (in
    /// [`ShapingScope::Global`] mode) or to a single
    /// round-robin-selected peer (in
    /// [`ShapingScope::PerConnection`] mode).
    ///
    /// [`ShapingScope::Global`]: crate::ShapingScope::Global
    /// [`ShapingScope::PerConnection`]: crate::ShapingScope::PerConnection
    pub fn broadcast_shaped(&self, payload: &[u8]) -> Result<[u8; ID_SIZE], Error> {
        self.shaper.enqueue(Lane::High, Target::Broadcast, payload)
    }

    /// Submits a real (application-originated) message to be
    /// shaped and queued on the *low*-priority lane. Useful
    /// for traffic that is real but lower-priority than
    /// [`send_shaped`](Node::send_shaped) /
    /// [`broadcast_shaped`](Node::broadcast_shaped) — for
    /// example, a freshly-received message that the node wants
    /// to re-broadcast promptly. A low-priority enqueue is
    /// popped on the very next shaping tick (LIFO discipline),
    /// but only if the high-priority lane is empty.
    pub fn send_shaped_low(
        &self,
        peer: SocketAddr,
        payload: &[u8],
    ) -> Result<[u8; ID_SIZE], Error> {
        self.shaper
            .enqueue(Lane::Low, Target::Unicast(peer), payload)
    }

    /// Submits a real (application-originated) message to be
    /// shaped and broadcast from the *low*-priority lane. The
    /// LIFO discipline means a fresh enqueue is popped on the
    /// very next shaping tick.
    pub fn broadcast_shaped_low(&self, payload: &[u8]) -> Result<[u8; ID_SIZE], Error> {
        self.shaper.enqueue(Lane::Low, Target::Broadcast, payload)
    }

    /// Submits a *pre-built* frame (already padded to
    /// `frame_size`) for broadcast from the low-priority lane.
    ///
    /// This is the right method for re-broadcasting a frame
    /// received from a peer *byte-for-byte unchanged* (so an
    /// intermediate node's dedup cache recognizes the same
    /// ID), or for protocols that want to use a different
    /// frame-construction convention than peashape's defaults.
    ///
    /// # Errors
    ///
    /// Returns [`Error::FrameSizeMismatch`] if the frame is not
    /// exactly `frame_size` bytes long.
    pub fn relay_shaped(&self, frame: BytesMut) -> Result<(), Error> {
        self.shaper.enqueue_raw(Lane::Low, Target::Broadcast, frame)
    }

    /// Returns a broadcast receiver that yields every frame
    /// received from a peer — whether it originated as a "real"
    /// message or as cover traffic is **not** observable on the
    /// wire (or by this method).
    ///
    /// The application is responsible for filtering cover frames;
    /// the conventional way to do so is to use a payload format
    /// with a recognizable structure (e.g. an AEAD tag, a
    /// version byte, or a magic header) that random cover bytes
    /// will not match by chance.
    pub fn subscribe(&self) -> broadcast::Receiver<BytesMut> {
        self.shaper.incoming().subscribe()
    }

    /// Initiates an outbound connection to a peer. If the node is
    /// already connected to that address, the call succeeds
    /// without taking further action.
    ///
    /// # Errors
    ///
    /// Returns any I/O error reported by `pea2pea` (e.g. the
    /// listener is not bound, the address is unreachable, the
    /// connection limits have been reached).
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.p2p.connect(addr).await
    }

    /// Closes the connection to a peer, if one is currently open.
    /// Returns `true` if a connection was actually torn down.
    pub async fn disconnect(&self, addr: SocketAddr) -> bool {
        self.p2p.disconnect(addr).await
    }

    /// Returns the addresses of currently-connected peers.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.p2p.connected_addrs()
    }

    /// Returns the bound listening address, or an error if the
    /// node was not configured with a listener.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::AddrNotAvailable`] if no listener
    /// is configured or if the listener has been toggled off.
    pub async fn local_addr(&self) -> io::Result<SocketAddr> {
        self.p2p.listening_addr().await
    }

    /// Returns a reference to the underlying `pea2pea::Node`.
    ///
    /// Exposed so that callers can layer additional `pea2pea`
    /// protocols on top of `peashape` (e.g. a custom `Handshake`)
    /// before calling [`Node::spawn`].
    pub fn p2p(&self) -> &P2pNode {
        &self.p2p
    }

    /// Returns the [`ShapeConfig`] this node was built from.
    pub fn config(&self) -> &ShapeConfig {
        self.shaper.config()
    }

    /// Returns a clone of the underlying [`Shaper`] handle.
    ///
    /// Exposed so that higher-level protocols (e.g. `peasub`)
    /// can enqueue directly into the priority lanes and
    /// subscribe to incoming frames without going through the
    /// `Node` wrapper.
    pub fn shaper(&self) -> Arc<Shaper> {
        self.shaper.clone()
    }

    /// Gracefully shuts the node down.
    ///
    /// Sets the shutdown flag (which the scheduler polls), wakes
    /// the scheduler out of any pending sleep, closes all
    /// connections, and aborts the `pea2pea` background tasks.
    /// Waits for the shaping task to finish. After `shutdown`
    /// returns the node is unusable; callers should drop it.
    ///
    /// `shutdown` is idempotent: calling it on an already-shut-down
    /// node is a no-op.
    pub async fn shutdown(&self) {
        self.shaper.shutting_down().store(true, Ordering::SeqCst);
        self.scheduler.wake();
        self.p2p.shut_down().await;
        // Drop the mutex guard before awaiting the handle to
        // satisfy `clippy::await_holding_lock`.
        let handle = self.scheduler_handle.lock().take();
        if let Some(handle) = handle {
            let _ = handle.await;
        }
    }
}

impl Pea2Pea for Node {
    fn node(&self) -> &P2pNode {
        &self.p2p
    }
}

impl Reading for Node {
    type Message = BytesMut;
    type Codec = Codec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Codec::new(
            self.shaper.config().frame_size,
            self.shaper.config().max_frame_size,
        )
    }

    async fn process_message(&self, _source: SocketAddr, message: Self::Message) {
        self.shaper.handle_incoming(message);
    }
}

impl Writing for Node {
    type Message = BytesMut;
    type Codec = Codec;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Codec::new(
            self.shaper.config().frame_size,
            self.shaper.config().max_frame_size,
        )
    }
}
