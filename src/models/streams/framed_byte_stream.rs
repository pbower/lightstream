use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_core::Stream;

use crate::traits::byte_stream::GenByteStream;
use crate::traits::stream_buffer::StreamBuffer;
use crate::traits::frame_decoder::FrameDecoder;
use crate::enums::DecodeResult;

/// Asynchronous streaming adapter for length-delimited or framed binary protocols (generic).
///
/// `FramedByteStreamGeneric` takes any chunked byte source (implementing `GenByteStream`)
/// and produces high-level frames using a user-supplied frame decoder (`FrameDecoder`).
///
/// # Purpose
/// - Converts a raw byte stream (arbitrary-sized chunks, possibly partial messages)
///   into a clean stream of protocol-level frames (e.g., Arrow IPC, Protobuf messages).
///
/// # Mechanism
/// - Maintains an internal buffer (`B`) which holds all unread bytes.
/// - Invokes the `FrameDecoder` repeatedly on the buffer, extracting and yielding
///   complete frames as soon as enough bytes are available.
/// - If insufficient data is available for a frame, automatically fetches more bytes
///   from the underlying source and appends to the buffer.
/// - Protocol errors or truncated frames at end-of-stream yield an error.
///
/// # Usage
/// - Construct via `FramedByteStreamGeneric::new(stream, decoder, initial_capacity)`.
/// - Poll as a `Stream<Item=Result<D::Frame, io::Error>>`.
/// - Suitable for streaming, async, and chunked IO contexts.
///
/// # Invariants
/// - The decoder **never allocates or mutates** the buffer beyond consumption.
/// - The buffer is only drained after successful frame extraction.
/// - On `Poll::Ready(None)` (end-of-stream), any residual bytes in the buffer are
///   treated as a truncated frame and return an error.
///
/// # Example
/// ```ignore
/// let byte_stream = ...; // Implements ByteStream or ByteStream64
/// let decoder = MyProtocolDecoder::new();
/// let mut stream = FramedByteStreamGeneric::new(byte_stream, decoder, 4096);
/// while let Some(frame) = stream.next().await { ... }
/// ```
pub struct FramedByteStream<S, D, B>
where
    S: GenByteStream<B>,
    D: FrameDecoder,
    B: StreamBuffer,
{
    /// The underlying byte source.
    pub(crate) inner: S,
    /// The stateful frame decoder.
    decoder: D,
    /// Rolling internal buffer holding unread/partial bytes.
    buf: B,
}

impl<S, D, B> FramedByteStream<S, D, B>
where
    S: GenByteStream<B>,
    D: FrameDecoder,
    B: StreamBuffer,
{
    /// Create a new framed byte stream with the specified decoder and buffer capacity.
    pub fn new(stream: S, decoder: D, initial_capacity: usize) -> Self {
        Self {
            inner: stream,
            decoder,
            buf: B::with_capacity(initial_capacity),
        }
    }
}

impl<S, D, B> Stream for FramedByteStream<S, D, B>
where
    S: GenByteStream<B>,
    D: FrameDecoder + Unpin,
    B: StreamBuffer + Unpin,
{
    type Item = Result<D::Frame, io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        loop {
            // Attempt to extract as many frames as possible from the buffer.
            match me.decoder.decode(me.buf.as_ref()) {
                Ok(DecodeResult::Frame { frame, consumed }) => {
                    // Remove consumed bytes from the buffer.
                    me.buf.drain(0..consumed);
                    return Poll::Ready(Some(Ok(frame)));
                }
                Ok(DecodeResult::NeedMore) => {
                    // Not enough bytes for a frame—read more data.
                }
                Err(e) => {
                    me.buf = B::default();
                    return Poll::Ready(Some(Err(e)));
                }
            }

            // Fetch the next chunk from the underlying byte stream.
            match Pin::new(&mut me.inner).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(chunk))) => {
                    me.buf.extend_from_slice(chunk.as_ref());
                    continue; // Retry decoding with more data.
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    // End-of-stream: if buffer still contains data, it's a protocol error.
                    if me.buf.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "truncated frame at end of stream",
                        ))));
                    }
                }
            }
        }
    }
}