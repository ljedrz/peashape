//! Error types returned by the public API.

use thiserror::Error;

/// All errors that can be surfaced to the application through the
/// public `peashape` API.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// An I/O error from the underlying `pea2pea` transport, e.g. a
    /// failed `connect`, `bind`, or socket read.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The application payload is too large to fit in a single
    /// frame. The maximum size is `frame_size` bytes. Either shrink
    /// the payload or raise `frame_size`.
    #[error("payload too large: {size} bytes, maximum is {max}")]
    PayloadTooLarge {
        /// The size of the offending payload, in bytes.
        size: usize,
        /// The maximum payload size accepted by this node, in
        /// bytes.
        max: usize,
    },

    /// The application priority lane is full. This happens when
    /// `send_shaped` / `broadcast_shaped` is called faster than the
    /// configured rate for a sustained period, so the queue of
    /// pending application messages has reached `lane_capacity`.
    /// The caller should either slow down, raise the rate, or
    /// raise `lane_capacity`.
    #[error("priority lane is full; the configured rate is too low for the current submit rate")]
    LaneFull,

    /// The requested target is not a connected peer.
    #[error("peer {0} is not connected")]
    NotConnected(std::net::SocketAddr),

    /// A pre-built frame handed to [`Shaper::enqueue_raw`] did
    /// not have the expected size.
    ///
    /// [`Shaper::enqueue_raw`]: crate::Shaper::enqueue_raw
    #[error("frame size mismatch: got {size} bytes, expected {expected}")]
    FrameSizeMismatch {
        /// The actual size of the supplied frame, in bytes.
        size: usize,
        /// The size the node was configured to expect, in bytes.
        expected: usize,
    },
}
