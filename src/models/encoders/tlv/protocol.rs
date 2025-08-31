use crate::models::frames::tlv_frame::TLVFrame;
use crate::traits::frame_encoder::FrameEncoder;
use crate::traits::stream_buffer::StreamBuffer;
use std::io;

pub struct TLVEncoder;

impl FrameEncoder for TLVEncoder {
    type Frame<'a> = TLVFrame<'a>;
    type Metadata = ();

    /// Encode a TLV frame as [type (1 byte)] [length (4 LE bytes)] [value bytes]
    fn encode<'a, B: StreamBuffer>(
        global_offset: &mut usize,
        frame: &Self::Frame<'a>,
    ) -> io::Result<(B, Self::Metadata)> {
        let len = frame.value.len();
        if len > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Value too large for TLV frame",
            ));
        }

        // 1 + 4 + value.len()
        let mut out = B::with_capacity(1 + 4 + len);
        out.push(frame.t);
        out.extend_from_slice(&(len as u32).to_le_bytes());
        *global_offset += 5; // type (1) + length (4)
        out.extend_from_slice(frame.value);
        *global_offset += frame.value.len();
        
        // Note: TLV padding is intentionally not implemented here as TLV is meant to be 
        // a simple wire format. Alignment padding would be more appropriate at a higher 
        // level protocol layer if needed for specific use cases (e.g., when embedding
        // TLV frames within Arrow IPC contexts).
        
        Ok((out, ()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use minarrow::Vec64;

    #[test]
    fn test_tlv_encode_basic() {
        let value = [0xAA, 0xBB, 0xCC];
        let frame = TLVFrame {
            t: 42,
            value: &value,
        };

        let (buf, _) = TLVEncoder::encode::<Vec64<u8>>(&mut 0, &frame).unwrap();
        assert_eq!(buf[0], 42);
        assert_eq!(&buf[1..5], &(3u32.to_le_bytes()));
        assert_eq!(&buf[5..8], &value);
        assert_eq!(buf.len(), 8);
    }

    #[test]
    fn test_tlv_encode_empty() {
        let frame = TLVFrame { t: 7, value: &[] };
        let (buf, _) = TLVEncoder::encode::<Vec64<u8>>(&mut 0, &frame).unwrap();
        assert_eq!(buf[0], 7);
        assert_eq!(&buf[1..5], &(0u32.to_le_bytes()));
        assert_eq!(buf.len(), 5);
    }

    #[test]
    fn test_tlv_encode_large() {
        let large = vec![1u8; 65536];
        let frame = TLVFrame {
            t: 1,
            value: &large,
        };
        let (buf, _) = TLVEncoder::encode::<Vec64<u8>>(&mut 0, &frame).unwrap();
        assert_eq!(buf[0], 1);
        assert_eq!(&buf[1..5], &(65536u32.to_le_bytes()));
        assert_eq!(&buf[5..5 + 65536], &large[..]);
        assert_eq!(buf.len(), 5 + 65536);
    }
}
