use minarrow::Vec64;

/// Abstraction over a byte buffer for frame-based streaming.
///
/// This trait defines the required interface for internal buffers used by `Minarrow` stream readers
/// and the IO framing layers that support them.
///
/// It is implemented for standard `Vec<u8>` and `minarrow::Vec64<u8>`, but can also be implemented
/// for alternative byte buffer types (e.g., Tokio's `Bytes`).
///
/// This allows integration of different lower-level byte abstractions with `Minarrow`'s `TableStreamReader` and `TableStreamWriter`,
/// supporting scenarios where the byte-level IO stack is fixed or externally controlled,
/// or where an alternative buffer implementation is preferred, enabling flexibility in the underlying
/// buffer representation without requiring changes to the framing logic.
///
/// Implementors must support the following standard `Vec` operations:
/// - Draining consumed bytes
/// - Appending new bytes
/// - Querying the current buffer length
/// - Accessing the internal byte slice
pub trait StreamBuffer: AsRef<[u8]> + Default + Extend<u8> + 'static {
    /// What alignment should the data buffer use?
    /// This is a data point that can be used for enforcing the alignment
    /// constraint via padding when necessary.
    const ALIGN: usize;

    /// Create with given capacity.
    fn with_capacity(n: usize) -> Self;

    // Reserves additional capacity in the buffer
    fn reserve(&mut self, additional: usize);

    /// Remove the specified range from the front of the buffer.
    fn drain(&mut self, range: std::ops::Range<usize>);
    /// Current length (in bytes).
    fn len(&self) -> usize;
    /// Whether the buffer is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Append bytes from a slice.
    fn extend_from_slice(&mut self, data: &[u8]);
    /// Push a single byte to the end of the buffer.
    fn push(&mut self, byte: u8);
    /// Create a buffer from a slice (copies the bytes).
    fn from_slice(data: &[u8]) -> Self;
}

impl StreamBuffer for Vec<u8> {
    // Common arrow ecosystem alignment
    const ALIGN: usize = 8;

    fn with_capacity(n: usize) -> Self {
        Vec::with_capacity(n)
    }

    fn reserve(&mut self, additional: usize) -> () {
        self.reserve(additional);
    }

    fn drain(&mut self, range: std::ops::Range<usize>) {
        // Call Vec::<T>::drain, ignore the returned Drain iterator
        Vec::<u8>::drain(self, range);
    }

    fn len(&self) -> usize {
        Vec::<u8>::len(self)
    }

    fn extend_from_slice(&mut self, data: &[u8]) {
        Vec::<u8>::extend_from_slice(self, data)
    }

    fn push(&mut self, byte: u8) {
        Vec::<u8>::push(self, byte)
    }

    fn from_slice(data: &[u8]) -> Self {
        data.to_vec()
    }
}

impl StreamBuffer for Vec64<u8> {
    /// Within this crate, affects SIMD alignment on Arrow IPC data buffers only.
    /// The rest of the flatbuffers metadata etc. gets 8-byte alignment,
    /// when used in an IPC encoding/decoding context.
    ///
    /// Basically - this hooks Vec64, which normally allocates to align its
    /// buffer starts to 64-byte boundaries (not hugely relevant at initialisation
    /// as part of a networked payload), to saying to this app: "pad me to 64 bytes".
    /// That way, the final allocation, which can be used via a zero-copy reference
    /// with `Minarrow` if desired (or any other framework) - you've got a SIMD-
    /// ready buffer allocations to immediately use on the Arrow byte buffer payloads.
    /// This excludes Flatbuffers metadata, which will always allocate to 8-byte
    /// boundaries when there is no need for them to take up more space, but,
    /// immediately preceding an arrow buffer, the implementation ensures the buffer
    /// starts on a 64-byte aligned point.
    const ALIGN: usize = 64;

    fn with_capacity(n: usize) -> Self {
        Vec64::with_capacity(n)
    }

    fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    fn drain(&mut self, range: std::ops::Range<usize>) {
        self.0.drain(range);
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn extend_from_slice(&mut self, data: &[u8]) {
        self.0.extend_from_slice(data)
    }
    fn push(&mut self, byte: u8) {
        self.0.push(byte)
    }
    fn from_slice(data: &[u8]) -> Self {
        Vec64::from_slice(data)
    }
}
