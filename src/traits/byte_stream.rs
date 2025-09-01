//! # Byte Stream Traits
//!
//! Lightweight, `futures`-compatible trait aliases for asynchronous, chunked byte
//! streams used throughout Lightstream.
//!
//! ## Goals
//! - **Interoperability:** Accept any `futures_core::Stream` that yields
//!   `Result<_, std::io::Error>`.
//! - **Zero-cost abstraction:** Pure trait alias pattern; no boxing, no dyn dispatch.
//! - **SIMD alignment** Foundational `ByteStream64` for 64-byte aligned
//!   buffers read and written via `minarrow::Vec64<u8>`. This helps to avoid additional
//!   re-allocations when the target is shortest path to SIMD-ready buffers. 
//!
//! ## Notes
//! - All traits require `Send + Unpin` to ensure compatibility with common async
//!   executors and to allow safe movement across tasks.
//! - Backpressure semantics are inherited from the underlying `Stream`
//!   implementation.
//!
//! The module provides **trait bounds** as low-level foundational primitive. 
//! Concrete stream implementations are supplied by callers (e.g. file/network sources,
//! framed transports, or in-memory producers).

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
/// This is a special case for scenarios where both the producer and consumer are under
/// your control and SIMD alignment is required end-to-end (e.g. disk or network paths that
/// preserve alignment). Using this avoids later re-allocation purely to fix alignment.
///
/// Implemented automatically for any [`Stream`] yielding `Result<Vec64<u8>, io::Error>`
/// and supporting `Send` + `Unpin`.
pub trait ByteStream64: Stream<Item = Result<Vec64<u8>, io::Error>> + Send + Unpin {}

impl<T> ByteStream64 for T where T: Stream<Item = Result<Vec64<u8>, io::Error>> + Send + Unpin {}

/// Generalised trait for any asynchronous, chunked byte stream of the given buffer type `B`.
///
/// This is a pure set of bounds that allows plugging in any compliant `Stream`
/// without dynamic dispatch.
///
/// Implemented for any `Stream<Item = Result<B, io::Error>> + Send + Unpin`.
pub trait GenByteStream<B>: Stream<Item = Result<B, io::Error>> + Send + Unpin {}

impl<T, B> GenByteStream<B> for T where T: Stream<Item = Result<B, io::Error>> + Send + Unpin {}
