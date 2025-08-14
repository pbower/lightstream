//! Parquet Decoding helpers

use std::io::Read;

use minarrow::Vec64;

use crate::error::IoError;

// Primitive decoders

pub fn decode_int32_plain(buf: &[u8]) -> Result<Vec64<i32>, IoError> {
    if buf.len() % 4 != 0 {
        return Err(IoError::Format("decode_int32_plain: buffer len % 4 != 0".into()));
    }
    Ok(buf.chunks_exact(4).map(|c| i32::from_le_bytes(c.try_into().unwrap())).collect())
}

pub fn decode_int64_plain(buf: &[u8]) -> Result<Vec64<i64>, IoError> {
    if buf.len() % 8 != 0 {
        return Err(IoError::Format("decode_int64_plain: buffer len % 8 != 0".into()));
    }
    Ok(buf.chunks_exact(8).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect())
}

pub fn decode_uint32_as_int32_plain(buf: &[u8]) -> Result<Vec64<u32>, IoError> {
    if buf.len() % 4 != 0 {
        return Err(IoError::Format("buffer len % 4 != 0".into()));
    }
    Ok(buf.chunks_exact(4).map(|c| u32::from_le_bytes(c.try_into().unwrap())).collect())
}

#[cfg(feature = "extended_numeric_types")]
pub fn decode_uint8_as_int32_plain(buf: &[u8]) -> Result<Vec64<u8>, IoError> {
    Ok(buf.iter().copied().collect())
}

#[cfg(feature = "extended_numeric_types")]
pub fn decode_uint16_as_int32_plain(buf: &[u8]) -> Result<Vec64<u16>, IoError> {
    if buf.len() % 2 != 0 {
        return Err(IoError::Format("buffer len % 2 != 0".into()));
    }
    Ok(buf.chunks_exact(2).map(|c| u16::from_le_bytes(c.try_into().unwrap())).collect())
}

pub fn decode_uint64_as_int64_plain(buf: &[u8]) -> Result<Vec64<u64>, IoError> {
    if buf.len() % 8 != 0 {
        return Err(IoError::Format("buffer len % 8 != 0".into()));
    }
    Ok(buf.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().unwrap())).collect())
}

pub fn decode_float32_plain(buf: &[u8]) -> Result<Vec64<f32>, IoError> {
    if buf.len() % 4 != 0 {
        return Err(IoError::Format("decode_float32_plain: buffer len % 4 != 0".into()));
    }
    Ok(buf.chunks_exact(4).map(|c| f32::from_le_bytes(c.try_into().unwrap())).collect())
}

pub fn decode_float64_plain(buf: &[u8]) -> Result<Vec64<f64>, IoError> {
    if buf.len() % 8 != 0 {
        return Err(IoError::Format("decode_float64_plain: buffer len % 8 != 0".into()));
    }
    Ok(buf.chunks_exact(8).map(|c| f64::from_le_bytes(c.try_into().unwrap())).collect())
}

// UTF-8 strings

pub fn decode_string_plain(
    buf: &[u8],
    len: usize
) -> Result<(Vec64<u32>, Vec64<u8>), IoError> {
    let mut offsets = Vec64::with_capacity(len + 1);
    offsets.push(0);
    let mut values = Vec64::new();
    let mut p = std::io::Cursor::new(buf);

    for _ in 0..len {
        let mut l4 = [0u8; 4];
        p.read_exact(&mut l4).map_err(|e| IoError::Format(e.to_string()))?;
        let l = u32::from_le_bytes(l4) as usize;
        let mut s = vec![0u8; l];
        p.read_exact(&mut s).map_err(|e| IoError::Format(e.to_string()))?;
        values.extend_from_slice(&s);
        offsets.push(values.len() as u32);
    }
    Ok((offsets, values))
}

#[cfg(feature = "large_string")]
pub fn decode_large_string_plain(
    buf: &[u8],
    len: usize
) -> Result<(Vec64<u64>, Vec64<u8>), IoError> {
    let mut offsets = Vec64::with_capacity(len + 1);
    offsets.push(0);
    let mut values = Vec64::new();
    let mut p = std::io::Cursor::new(buf);

    for _ in 0..len {
        let mut l4 = [0u8; 4];
        p.read_exact(&mut l4).map_err(|e| IoError::Format(e.to_string()))?;
        let l = u32::from_le_bytes(l4) as usize;
        let mut s = vec![0u8; l];
        p.read_exact(&mut s).map_err(|e| IoError::Format(e.to_string()))?;
        values.extend_from_slice(&s);
        offsets.push(values.len() as u64);
    }
    Ok((offsets, values))
}

// temporal aliases
pub fn decode_datetime32_plain(buf: &[u8]) -> Result<Vec64<i32>, IoError> {
    decode_int32_plain(buf)
}
pub fn decode_datetime64_plain(buf: &[u8]) -> Result<Vec64<i64>, IoError> {
    decode_int64_plain(buf)
}

// Dictionary indices decoder

/// Decode the RLE_DICTIONARY index stream.
///
/// * `len` – number of logical indices expected.
pub fn decode_dictionary_indices_rle(buf: &[u8], len: usize) -> Result<Vec64<u32>, IoError> {
    if buf.is_empty() {
        return Err(IoError::Format("empty dictionary index stream".to_owned()));
    }
    let bit_width = buf[0];
    if !(1..=32).contains(&bit_width) {
        return Err(IoError::Format(format!("invalid bit width {bit_width}")));
    }
    let mut input = &buf[1..];
    let mut out = Vec64::with_capacity(len);

    let bytes_per_val = ((bit_width + 7) / 8) as usize;

    while out.len() < len {
        // ---- read ULEB128 header --------------------------------------
        let (header, consumed) = {
            let mut val: u64 = 0;
            let mut shift = 0;
            let mut read = 0;
            for &b in input {
                val |= ((b & 0x7f) as u64) << shift;
                read += 1;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
                if shift >= 64 {
                    return Err(IoError::Format("ULEB128 too large".into()));
                }
            }
            (val, read)
        };
        input = &input[consumed..];

        if header & 1 == 0 {
            // --------------- RLE run ------------------------------------
            let run_len = (header >> 1) as usize;
            if input.len() < bytes_per_val {
                return Err(IoError::Format("truncated RLE value".into()));
            }
            let mut v_bytes = [0u8; 4];
            v_bytes[..bytes_per_val].copy_from_slice(&input[..bytes_per_val]);
            let value = u32::from_le_bytes(v_bytes) & ((1u32 << bit_width) - 1);
            input = &input[bytes_per_val..];

            let needed = len - out.len();
            out.extend(std::iter::repeat(value).take(run_len.min(needed)));
        } else {
            // --------------- Bit-packed run -----------------------------
            let groups = (header >> 1) as usize; // 1 group = 8 values
            let bytes_in_run = groups * bit_width as usize;
            if input.len() < bytes_in_run {
                return Err(IoError::Format("truncated bit-packed run".into()));
            }
            // decode all groups in this run 
            let mut scratch = vec![0u32; groups * 8];
            for bit in 0..bit_width {
                for g in 0..groups {
                    let b = input[bit as usize * groups + g];
                    for j in 0..8 {
                        let idx = g * 8 + j;
                        if idx < scratch.len() && (b >> j) & 1 != 0 {
                            scratch[idx] |= 1 << bit;
                        }
                    }
                }
            }
            let needed = len - out.len();
            for v in scratch.into_iter().take(needed) {
                out.push(v);
            }
            input = &input[bytes_in_run..];
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use crate::models::encoders::parquet::data::encode_dictionary_indices_rle;

    use super::*;
    // for round‐trip against the encoder

    #[test]
    fn test_decode_int32_plain_ok() {
        // [1, -1]
        let buf: &[u8] = &[1, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];
        let v = decode_int32_plain(buf).unwrap();
        assert_eq!(v.0, vec![1, -1]);
    }

    #[test]
    fn test_decode_int32_plain_err() {
        let buf = &[1, 2, 3]; // not a multiple of 4
        let err = decode_int32_plain(buf).unwrap_err();
        matches!(err, IoError::Format(_));
    }

    #[test]
    fn test_decode_uint32_as_int32_plain() {
        let buf: &[u8] = &[1, 0, 0, 0, 2, 0, 0, 0];
        let v = decode_uint32_as_int32_plain(buf).unwrap();
        assert_eq!(v.0, vec![1u32, 2u32]);
    }

    #[test]
    fn test_decode_uint64_as_int64_plain() {
        let buf: &[u8] = &1u64.to_le_bytes();
        let v = decode_uint64_as_int64_plain(buf).unwrap();
        assert_eq!(v.0, vec![1u64]);
    }

    #[test]
    fn test_decode_float32_plain() {
        let a = 1.5f32;
        let b = -2.25f32;
        let mut buf = Vec::new();
        buf.extend_from_slice(&a.to_le_bytes());
        buf.extend_from_slice(&b.to_le_bytes());
        let v = decode_float32_plain(&buf).unwrap();
        assert_eq!(v.0, vec![a, b]);
    }

    #[test]
    fn test_decode_float64_plain() {
        let a = 3.14159f64;
        let b = -0.125f64;
        let mut buf = Vec::new();
        buf.extend_from_slice(&a.to_le_bytes());
        buf.extend_from_slice(&b.to_le_bytes());
        let v = decode_float64_plain(&buf).unwrap();
        assert_eq!(v.0, vec![a, b]);
    }

    #[test]
    fn test_decode_string_plain_simple() {
        // two strings: "hi", "!"
        // lengths 2,1  and bytes [h,i,!,]
        let mut buf = Vec::new();
        buf.extend_from_slice(&(2u32.to_le_bytes())); // "hi"
        buf.extend_from_slice(b"hi");
        buf.extend_from_slice(&(1u32.to_le_bytes())); // "!"
        buf.extend_from_slice(b"!");
        let (offsets, values) = decode_string_plain(&buf, 2).unwrap();
        // offsets at [0,2,3], values=b"hi!"
        assert_eq!(offsets.0, vec![0, 2, 3]);
        assert_eq!(values.0, b"hi!".to_vec());
    }

    #[test]
    fn test_decode_string_plain_err_truncated() {
        // declare length=5 but only 3 bytes present
        let mut buf = Vec::new();
        buf.extend_from_slice(&(5u32.to_le_bytes()));
        buf.extend_from_slice(b"abc");
        let err = decode_string_plain(&buf, 1).unwrap_err();
        matches!(err, IoError::Format(_));
    }

    #[test]
    fn test_decode_datetime_aliases() {
        let buf: &[u8] = &[0x01, 0, 0, 0];
        let v32 = decode_datetime32_plain(buf).unwrap();
        let v64 = decode_datetime64_plain(&[0x02, 0, 0, 0, 0, 0, 0, 0]).unwrap();
        assert_eq!(v32.0, vec![1]);
        assert_eq!(v64.0, vec![2]);
    }

    #[test]
    fn test_decode_dictionary_rle_run_only() {
        // bit_width=8, header=(3<<1)=6, value=42, len=3
        let buf = &[8u8, 6, 42];
        let out = decode_dictionary_indices_rle(buf, 3).unwrap();
        assert_eq!(out.0, vec![42, 42, 42]);
    }

    #[test]
    fn test_decode_dictionary_rle_partial_run() {
        // run of 5 but len=3 → only 3 outputs
        let buf = &[8u8, 10, 7]; // header=10 (5<<1), value=7
        let out = decode_dictionary_indices_rle(buf, 3).unwrap();
        assert_eq!(out.0, vec![7, 7, 7]);
    }

    #[test]
    fn test_decode_dictionary_rle_bitpacked() {
        // bit_width=1, header=(1<<1)|1=3, single group byte=0b01010101
        let buf = &[1u8, 3, 0b01010101];
        let out = decode_dictionary_indices_rle(buf, 8).unwrap();
        assert_eq!(out.0, vec![1, 0, 1, 0, 1, 0, 1, 0]);

        // truncated to len=5
        let out2 = decode_dictionary_indices_rle(buf, 5).unwrap();
        assert_eq!(out2.0, vec![1, 0, 1, 0, 1]);
    }

    #[test]
    fn test_decode_dictionary_rle_invalid() {
        assert!(matches!(decode_dictionary_indices_rle(&[], 1), Err(IoError::Format(_))));
        // bad bit_width
        let buf = &[0u8];
        assert!(matches!(decode_dictionary_indices_rle(buf, 1), Err(IoError::Format(_))));
    }

    #[test]
    fn test_decode_dictionary_roundtrip_via_encode() {
        let indices = vec![0, 1, 1, 2, 2, 2, 2, 2, 3, 3];
        // prepare a buffer for the encoder
        let mut buf = Vec::new();
        // write into `buf` in-place
        encode_dictionary_indices_rle(&indices, &mut buf).unwrap();
        // now decode it back
        let out = decode_dictionary_indices_rle(&buf, indices.len()).unwrap();
        assert_eq!(out.0, indices);
    }
}
