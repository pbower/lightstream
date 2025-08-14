use crate::traits::stream_buffer::StreamBuffer;

/// The outcome of a single frame decoder step.
///
/// This enum communicates whether a full frame has been detected, whether more bytes are required,
/// or whether a protocol error has occurred.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DecodeResult<F> {
    /// A complete logical frame was detected.
    /// - `frame`: The decoded frame (of the target type).
    /// - `consumed`: The number of bytes from the buffer that should be discarded.
    Frame { frame: F, consumed: usize },

    /// The decoder requires more bytes to detect a full frame.
    /// - No bytes should be removed from the buffer.
    NeedMore,
}

/// Arrow IPC decoding state. Encodes all protocol and marker consumption.
#[derive(Debug, Clone)]
pub enum DecodeState<B: StreamBuffer> {
    Initial,
    AfterMagic,                  // After reading file magic header (File only)
    AfterContMarker,             // After reading stream continuation marker (Stream only)
    ReadingContinuationSize,
    ReadingMessageLength,        // Ready to read message length (prefix)
    ReadingMessage { msg_len: usize },
    ReadingBody { body_len: usize, message: B },
    ReadingFooter { footer_len: usize, footer_offset: usize },
    Done,
}

/// Specifies chunk sizing strategies for `DiskByteStream` and other stream sources.
///
/// Preset domain-appropriate buffer sizes.
///
/// # Variants:
/// - `FileIO`: 1 MiB chunks (default for disk reads).
/// - `Http`: 64 KiB (typical HTTP frame size).
/// - `WebSocket`: 32 KiB (WebSocket frame guideline).
/// - `WebTransport`: 64 KiB (QUIC optimised).
/// - `InMemory`: 512 KiB (RAM-local streams).
/// - `Custom(n)`: Explicit `n` bytes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BufferChunkSize {
    FileIO,
    Http,
    WebSocket,
    WebTransport,
    InMemory,
    Custom(usize),
}

impl BufferChunkSize {
    /// Returns the configured chunk size in bytes.
    pub fn chunk_size(self) -> usize {
        match self {
            BufferChunkSize::FileIO => 1 * 1024 * 1024,        // 1 MiB
            BufferChunkSize::Http => 64 * 1024,                // 64 KiB
            BufferChunkSize::WebSocket => 32 * 1024,           // 32 KiB
            BufferChunkSize::WebTransport => 64 * 1024,        // 64 KiB
            BufferChunkSize::InMemory => 512 * 1024,           // 512 KiB
            BufferChunkSize::Custom(n) => n,
        }
    }
}

/// Arrow framing protocol. There are 2 - one for files,
/// and one for unbounded streams, which each include different 
/// termination markers, in line with the official *Apache Arrow*
/// [IPC Streaming specification](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IPCMessageProtocol {
    Stream,
    File
}

/// Arrow Message Type Enum.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    Schema,
    RecordBatch,
    DictionaryBatch,
    Unknown
}

/// Stream message batch states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BatchState {
    NeedSchema,
    Ready,
    Done
}

/// Stream writer states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriterState {
    Fresh,
    SchemaDone,
    Closed
}
