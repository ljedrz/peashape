//! Frame-construction helpers and the on-the-wire ID convention.

use bytes::{BufMut, BytesMut};
use rand::RngExt;

use crate::config::ShapeConfig;

/// Size, in bytes, of the message-identifier field that prefixes
/// every frame in protocols built on top of `peashape`.
///
/// `peashape` itself does **not** interpret this field — the
/// underlying payload is opaque to the shaper. The convention
/// exists so that higher-level protocols (e.g. `peasub`) can
/// reserve a fixed-width random ID at the front of every frame
/// for deduplication and downstream correlation, without
/// having to negotiate the layout.
///
/// Applications that do not need an ID are free to ignore it
/// and put their own structure in the first 32 bytes.
pub const ID_SIZE: usize = 32;

/// Pads `payload` to `config.frame_size` with random bytes and
/// returns the freshly-assembled frame along with the random ID
/// written into its first [`ID_SIZE`] bytes.
///
/// The first [`ID_SIZE`] bytes are a random identifier; the
/// application's bytes follow; the trailing region (if any) is
/// random padding. The resulting frame is exactly
/// `config.frame_size` bytes long.
pub fn build_frame(config: &ShapeConfig, payload: &[u8]) -> ([u8; ID_SIZE], BytesMut) {
    let id: [u8; ID_SIZE] = rand::rng().random();
    let mut msg = BytesMut::with_capacity(config.frame_size);
    msg.extend_from_slice(&id);
    msg.extend_from_slice(payload);
    let pad = config.frame_size - ID_SIZE - payload.len();
    if pad > 0 {
        let mut rng = rand::rng();
        for _ in 0..pad {
            msg.put_u8(rng.random());
        }
    }
    debug_assert_eq!(msg.len(), config.frame_size);
    (id, msg)
}

/// Returns a freshly-generated cover frame: a random
/// [`ID_SIZE`]-byte identifier followed by `frame_size - ID_SIZE`
/// random bytes. Indistinguishable on the wire from a real
/// application message.
pub fn random_cover(config: &ShapeConfig) -> BytesMut {
    let mut msg = BytesMut::with_capacity(config.frame_size);
    let id: [u8; ID_SIZE] = rand::rng().random();
    msg.extend_from_slice(&id);
    let mut rng = rand::rng();
    for _ in 0..(config.frame_size - ID_SIZE) {
        msg.put_u8(rng.random());
    }
    msg
}
