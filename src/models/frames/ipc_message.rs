use crate::traits::stream_buffer::StreamBuffer;

/// Arrow IPC message component of the frame
///
/// This is a lower level construct for encoding Arrow files
/// for IO messaging across file and/or network layers.
#[derive(Debug)]
pub struct ArrowIPCMessage<B: StreamBuffer> {
    /// Arrow Flatbuffers message
    pub message: B,
    /// The Arrow data buffer payload
    pub body: B,
}

#[derive(Default)]
pub struct IPCFrameMetadata {
    pub header_len: usize,
    pub meta_len: usize,
    pub meta_pad: usize,
    pub body_len: usize,
    pub body_pad: usize,
    pub eos_len: usize,
    pub footer_len: usize,
    pub magic_len: usize,
}

impl IPCFrameMetadata {
    pub fn frame_len(&self) -> usize {
        self.header_len
            + self.metadata_total_len()
            + self.body_total_len()
            + self.footer_eos_len()
            + self.magic_len
    }

    pub fn metadata_total_len(&self) -> usize {
        self.meta_len + self.meta_pad
    }

    pub fn body_total_len(&self) -> usize {
        self.body_len + self.body_pad
    }

    pub fn footer_eos_len(&self) -> usize {
        self.eos_len + self.footer_len
    }
}
