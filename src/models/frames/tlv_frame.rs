use crate::traits::stream_buffer::StreamBuffer;

/// Type length value frame for encoding
///
/// `length` is not here as
/// that's handled through the protocol
/// via the `.len()` attribute at serialisation
/// time.
pub struct TLVFrame<'a> {
    pub t: u8,
    pub value: &'a [u8],
}

/// Type length message for decoding
///
/// `length` is not here as
/// that's handled through the protocol
/// via the `.len()` attribute during deserialisation.
pub struct TLVDecodedFrame<B: StreamBuffer> {
    pub t: u8,
    pub value: B,
}
