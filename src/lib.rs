pub mod traits {
    pub mod byte_stream;
    pub mod frame_decoder;
    pub mod frame_encoder;
    pub mod stream_buffer;
}

pub mod models {

    pub mod sinks {
        pub mod table_sink;
        pub mod tlv_sink;
    }
    pub mod encoders {
        pub mod ipc {
            pub mod protocol;
            pub mod schema;
            pub mod table_stream;
        }
        pub mod parquet {
            pub mod data;
            pub mod metadata;
        }
        pub mod tlv {
            pub mod protocol;
            pub mod tlv_stream;
        }
        pub mod csv;
    }
    pub mod decoders {
        pub mod ipc {
            pub mod parser;
            pub mod protocol;
            pub mod table_stream;
        }
        pub mod csv;
        pub mod parquet;
        pub mod tlv;
    }
    pub mod frames {
        pub mod ipc_message;
        pub mod tlv_frame;
    }
    pub mod readers {
        pub mod ipc {
            pub mod file_table_reader;
            pub mod mmap_table_reader;
            pub mod table_reader;
            pub mod table_stream_reader;
        }
        pub mod csv_reader;
        pub mod parquet_reader;
    }
    pub mod writers {
        pub mod ipc {
            pub mod table_stream_writer;
            pub mod table_writer;
        }
        pub mod csv_writer;
        pub mod parquet_writer;
    }
    pub mod streams {
        pub mod disk;
        pub mod framed_byte_stream;
    }
    pub mod types {
        pub mod parquet;
    }
    pub mod mmap;
}

pub mod arrow {
    pub mod file;
    pub mod message;
    pub mod schema;
}

pub mod compression;
pub mod constants;
pub mod enums;
pub mod error;
pub mod utils;

#[cfg(test)]
pub(crate) mod test_helpers;
pub use crate::arrow::message::org::apache::arrow::flatbuf::Message as AFMessage;
pub use crate::arrow::message::org::apache::arrow::flatbuf::MessageHeader as AFMessageHeader;
