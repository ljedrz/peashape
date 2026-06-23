//! Length-delimited framing with a fixed payload size.
//!
//! Every frame on the wire is a 4-byte big-endian length prefix
//! followed by exactly `frame_size` bytes of payload. The codec
//! rejects any frame whose payload size doesn't match — that is
//! what enforces the constant-size property at the framing level.

use std::io;

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

/// A `pea2pea` codec that constrains every frame on the wire to
/// a single, fixed payload size, regardless of the actual payload
/// contents.
///
/// # Wire format
///
/// ```text
/// +--------+------------+
/// | length |  payload   |
/// | (4 B)  | (frame_size)|
/// +--------+------------+
/// ```
///
/// - `length`: a 4-byte big-endian unsigned integer. Always equal
///   to `frame_size` for a well-formed frame; any other value
///   causes the connection to be torn down by the decoder.
/// - `payload`: exactly `frame_size` bytes of frame data. The
///   application is free to lay them out however it likes — random
///   padding, structured records, or anything in between — because
///   an observer cannot tell "what" is being sent, only "when",
///   "to whom", and "how big" (and the last is constant).
pub struct Codec {
    inner: LengthDelimitedCodec,
    frame_size: usize,
}

impl Codec {
    /// Creates a codec that accepts frames of exactly
    /// `frame_size` payload bytes and rejects any frame larger
    /// than `max_frame_size` bytes (defensive DoS bound).
    ///
    /// # Panics
    ///
    /// Panics if `frame_size == 0` (the resulting codec would
    /// never produce a useful message).
    pub fn new(frame_size: usize, max_frame_size: usize) -> Self {
        assert!(frame_size > 0, "frame_size must be non-zero");
        let inner = LengthDelimitedCodec::builder()
            .max_frame_length(max_frame_size)
            .length_field_length(4)
            .big_endian()
            .new_codec();
        Self { inner, frame_size }
    }

    /// Returns the on-the-wire payload size this codec expects.
    pub fn frame_size(&self) -> usize {
        self.frame_size
    }
}

impl Decoder for Codec {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.inner.decode(src)? {
            Some(buf) => {
                if buf.len() != self.frame_size {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "unexpected frame size: got {}, want {}",
                            buf.len(),
                            self.frame_size
                        ),
                    ));
                }
                Ok(Some(buf))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<BytesMut> for Codec {
    type Error = io::Error;

    fn encode(&mut self, item: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if item.len() != self.frame_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "unexpected frame size: got {}, want {}",
                    item.len(),
                    self.frame_size
                ),
            ));
        }
        // `LengthDelimitedCodec::encode` is generic over `Into<Bytes>`;
        // freeze our `BytesMut` so it is taken by-value without a copy.
        self.inner.encode(item.freeze(), dst)
    }
}
