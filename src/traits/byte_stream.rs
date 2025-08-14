use futures_core::Stream;
use minarrow::Vec64;
use std::io;

/// Universal trait alias for any asynchronous, chunked byte stream.
///
/// Implemented automatically for any [`Stream`] yielding `Result<Vec<u8>, io::Error>`
/// and supporting `Send` + `Unpin`.
pub trait ByteStream: Stream<Item = Result<Vec<u8>, io::Error>> + Send + Unpin {}

impl<T> ByteStream for T where T: Stream<Item = Result<Vec<u8>, io::Error>> + Send + Unpin {}

/// Universal trait alias for any asynchronous, chunked byte stream of 64-byte aligned buffers.
///
/// This is a special case for when one controls both the source and sender, and wants 
/// to ensure data buffers are SIMD aligned, for example, from disk, or over the network.
/// This prevents the need to re-allocate data later in the process.
/// 
/// Implemented automatically for any [`Stream`] yielding `Result<Vec64<u8>, io::Error>`
/// and supporting `Send` + `Unpin`. 
pub trait ByteStream64: Stream<Item = Result<Vec64<u8>, io::Error>> + Send + Unpin {}

impl<T> ByteStream64 for T where T: Stream<Item = Result<Vec64<u8>, io::Error>> + Send + Unpin {}

/// Generalised trait for any asynchronous, chunked byte stream of the given buffer type.
///
/// Because it is (only) a set of bounds, one can plug in a compliant type without dynamic dispatch.
/// 
/// Implemented for any `Stream<Item=Result<B, io::Error>>` + Send + Unpin.
pub trait GenByteStream<B>: Stream<Item = Result<B, io::Error>> + Send + Unpin {}
impl<T, B> GenByteStream<B> for T where T: Stream<Item = Result<B, io::Error>> + Send + Unpin {}
