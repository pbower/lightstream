//! Generic frame decoding infrastructure.
//!
//! For implementing minimally-allocating, incremental frame decoders over arbitrary byte streams.
//! 
//!  In this context, a frame refers to a self-contained unit of data extracted from a byte stream, where:
//!  - The header (or prefix) specifies the size or format of the upcoming payload.
//!  - The body (payload) contains exactly the number of bytes the header declares.
//!
//! A `FrameDecoder` is responsible for detecting protocol frame boundaries within a buffer slice
//! and reporting when a full logical frame has been received. It does not allocate or retain data;
//! it only inspects the passed-in buffer and returns how many bytes should be consumed.
//
//  Usage:
//  - Call `decode(&mut self, &[u8])` repeatedly as more bytes arrive.
//  - On `Frame { ... }`, consume the reported number of bytes and process the returned frame.
//  - On `NeedMore`, retain all bytes in the buffer and supply more data.
//  - On `Err`, treat as protocol violation or irrecoverable stream error.

use std::io;
use crate::enums::DecodeResult;

/// A trait for non-allocating, pull-based frame decoders.
///
/// Implement this trait for any wire format requiring message boundary detection,
/// such as Arrow IPC, protobuf, or custom binary protocols.
///
/// The decoder should never allocate or retain buffer data.
/// It must *only* inspect the provided slice and return either a complete frame
/// and how many bytes were consumed, or indicate that more data is needed.
///
/// ### Safety Contract
/// - The decoder musn't mutate or take ownership of the input buffer.
/// - It musn't remove bytes itself—return `consumed`, the caller will drop them.
/// - It should always leave the buffer unchanged if returning `NeedMore`.
pub trait FrameDecoder {
    /// The type of frame yielded by this decoder.
    type Frame;

    /// Attempt to decode a complete frame from the start of `buf`.
    ///
    /// Return:
    /// - `Ok(Frame { frame, consumed })` if a full frame is present. Caller removes `consumed` bytes.
    /// - `Ok(NeedMore)` if more bytes are required; buffer remains unchanged.
    /// - `Err` if the protocol is violated, or an unrecoverable error is detected.
    fn decode(&mut self, buf: &[u8]) -> io::Result<DecodeResult<Self::Frame>>;
}
