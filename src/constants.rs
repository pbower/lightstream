// --- Constants for IPC format ---

pub const DEFAULT_FRAME_ALLOCATION_SIZE: usize = 1024 * 1024; // 1 MB 

pub const ARROW_MAGIC_NUMBER_PADDED: &[u8] = b"ARROW1\0\0"; // opening magic
pub const ARROW_MAGIC_NUMBER: &[u8] = b"ARROW1"; // closing magic
pub const FILE_OPENING_MAGIC_LEN: usize = 8;
pub const FILE_CLOSING_MAGIC_LEN: usize = 6;
pub const EOS_MARKER_LEN: usize = 8; // 8 bytes cont 0xFFFFFFFF + 0u32.
pub const CONTINUATION_MARKER_LEN: usize = 4; // 4 bytes - <continuation: 0xFFFFFFFF>
pub const CONTINUATION_SENTINEL: u32 = 0xFFFF_FFFF;
pub const METADATA_SIZE_PREFIX: usize = 4; // 4 bytes - <metadata_size: int32>
