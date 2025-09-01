//! # Frame Encoder - **Generic frame encoding infrastructure**
//!
//! ## Purpose
//! For implementing incremental frame encoders over arbitrary byte sinks.
//!
//! A `FrameEncoder` is responsible for serialising a logical frame into a provided buffer,
//! according to the wire protocol (e.g., length-prefix, TLV, Arrow IPC, etc).
//! The encoder writes into a supplied buffer and does not allocate or retain data.
//!
//! ## Usage
//! - Call `encode(&self, &Frame, &mut Vec<u8>)` to serialise a frame.
//! - The encoder appends the wire format into the buffer; caller sends or stores as required.
//! - Encoders must not retain or mutate the frame after encoding.

use std::io;

use crate::traits::stream_buffer::StreamBuffer;

/// Implement this trait for any wire format requiring message serialisation,
/// such as Arrow IPC, protobuf, or custom binary protocols.
///
/// The encoder must only append to the provided buffer and must not retain references
/// or have side-effects to any data passed in.
///
/// ### Safety Contract
/// - The encoder must not mutate the frame being encoded.
/// - The encoder must not retain references to input data after the call.
/// - All writes must be bounded to the provided buffer.
pub trait FrameEncoder {
    /// The type of frame accepted by this encoder.
    type Frame<'a>;

    /// The type of metadata produced by this encoder.
    type Metadata;

    /// Encode a frame, producing both an output buffer and frame metadata.
    ///
    /// Returns an owned buffer containing the encoded frame and the associated metadata.
    /// Returns `Err` if encoding fails.
    ///
    /// ### Args
    /// * `global_offset`: keeps track of the pointer position across frames
    /// * `frame`: the frame being encoded
    fn encode<'a, B: StreamBuffer>(
        global_offset: &mut usize,
        frame: &Self::Frame<'a>,
    ) -> io::Result<(B, Self::Metadata)>;
}
