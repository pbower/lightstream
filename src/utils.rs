use tokio::io;

use crate::traits::stream_buffer::StreamBuffer;

/// Strip the optional Arrow streaming “continuation marker”.
///
/// In streaming IPC each logical message may be preceded by eight bytes:
///
/// ```text
///   0xFFFF_FFFF  – 4‑byte sentinel
///   N            – 32‑bit little‑endian size of the actual FlatBuffer
/// ```
///
/// If the prefix is present we return the slice *after* the marker.
/// Otherwise we return the original slice unchanged.
///
/// Returns `Err` when the prefix is present but truncated.
#[inline]
pub fn strip_continuation_prefix(buf: &[u8]) -> io::Result<&[u8]> {
    const SENTINEL: u32 = 0xFFFF_FFFF;
    if buf.len() >= 8 {
        let (prefix, rest) = buf.split_at(8);
        let marker = u32::from_le_bytes(prefix[..4].try_into().unwrap());
        if marker == SENTINEL {
            let announced = u32::from_le_bytes(prefix[4..8].try_into().unwrap()) as usize;
            if rest.len() < announced {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "continuation marker size exceeds remaining buffer",
                ));
            }
            return Ok(&rest[..announced]);
        }
    }
    Ok(buf)
}

/// Aligns the stream buffer to its alignment boundary
// #[inline(always)]
// pub fn align_to<B: StreamBuffer>(n: usize) -> usize {
//     let rem = n % B::ALIGN;
//     if rem == 0 { 0 } else { B::ALIGN - rem }
// }

#[inline(always)]
pub fn align_to<B: StreamBuffer>(n: usize) -> usize {
    let rem = n % B::ALIGN;
    if rem == 0 { 0 } else { B::ALIGN - rem }
}

/// Aligns the incoming length to 8 (typically byte) boundary
#[inline]
pub fn align_8(n: usize) -> usize {
    let rem = n % 8;
    if rem == 0 { 0 } else { 8 - rem }
}

/// Convert a typed slice to a byte slice for serialisation.
#[inline(always)]
pub fn as_bytes<T: Copy>(buf: &[T]) -> &[u8] {
    unsafe {
        std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len() * std::mem::size_of::<T>())
    }
}

// Println for debug mode for inspecting binary payloads, etc.
#[macro_export]
macro_rules! debug_println {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        {
            println!($($arg)*);
        }
    };
}

// Helper supporting dictionary columns for tables
pub (crate) fn extract_dictionary_values_from_col(col: &minarrow::FieldArray) -> Option<Vec<String>> {
    use minarrow::TextArray::*;
    match &col.array {
        minarrow::Array::TextArray(Categorical32(arr)) => Some(arr.unique_values.iter().cloned().collect()),
        #[cfg(feature = "extended_categorical")]
        minarrow::Array::TextArray(Categorical8(arr)) => Some(arr.unique_values.iter().cloned().collect()),
        #[cfg(feature = "extended_categorical")]
        minarrow::Array::TextArray(Categorical16(arr)) => Some(arr.unique_values.iter().cloned().collect()),
        #[cfg(feature = "extended_categorical")]
        minarrow::Array::TextArray(Categorical64(arr)) => Some(arr.unique_values.iter().cloned().collect()),
        _ => None
    }
}


/// Write Parquet-compliant bit-packed Boolean buffer for a sequence of booleans.
/// Output is LSB0 (least significant bit first, 8 booleans per byte).
pub fn write_parquet_bool_bits<I>(iter: I, len: usize, buf: &mut Vec<u8>)
where
    I: Iterator<Item = bool>,
{
    let packed = pack_bits(iter, len);
    buf.extend_from_slice(&packed);
}

/// Read Parquet-compliant bit-packed Boolean buffer to Vec<bool>.
pub fn read_parquet_bool_bits(buf: &[u8], len: usize) -> Vec<bool> {
    unpack_bits(buf, len)
}

/// Packs a sequence of bools into a bit-packed buffer (LSB0).
/// Returns a new Vec<u8>.
pub fn pack_bits<I>(iter: I, len: usize) -> Vec<u8>
where
    I: Iterator<Item = bool>,
{
    let n_bytes = (len + 7) / 8;
    let mut buf = vec![0u8; n_bytes];
    for (i, v) in iter.enumerate().take(len) {
        if v {
            buf[i / 8] |= 1 << (i % 8);
        }
    }
    buf
}

/// Unpacks a bit-packed buffer into a Vec<bool>, up to given length.
pub fn unpack_bits(buf: &[u8], len: usize) -> Vec<bool> {
    (0..len).map(|i| ((buf[i / 8] >> (i % 8)) & 1) != 0).collect()
}

