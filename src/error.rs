//! Error types for parquet IO

use std::string::FromUtf8Error;
use std::{error, fmt, io};

/// Unified error type for all parquet_writer operations.
#[derive(Debug)]
pub enum IoError {
    /// I/O failure (write error, flush error, file system error).
    Io(io::Error),

    /// Input table or column contains unsupported or mismatched types.
    UnsupportedType(String),

    /// Null mask or definition level inconsistency detected.
    NullMaskInconsistent(String),

    /// Malformed or invalid input data (UTF-8 error, bounds error, etc).
    MinarrowError(String),

    /// Metadata or schema violation.
    Metadata(String),

    /// Compression codec error.
    Compression(String),

    /// Internal logic error (should never occur).
    Internal(String),

    /// Data Formatting error
    Format(String),

    /// Unsupported encoding error
    UnsupportedEncoding(String),
}

impl fmt::Display for IoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IoError::Io(e) => write!(f, "I/O error: {}", e),
            IoError::UnsupportedType(s) => write!(f, "Unsupported type: {}", s),
            IoError::NullMaskInconsistent(s) => write!(f, "Null mask inconsistency: {}", s),
            IoError::MinarrowError(s) => write!(f, "Data error: {}", s),
            IoError::Metadata(s) => write!(f, "Metadata error: {}", s),
            IoError::Compression(s) => write!(f, "Compression error: {}", s),
            IoError::Internal(s) => write!(f, "Internal error: {}", s),
            IoError::Format(s) => write!(f, "Formatting error: {}", s),
            IoError::UnsupportedEncoding(s) => write!(f, "Unsupported encoding error: {}", s),
        }
    }
}

impl error::Error for IoError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            IoError::Io(e) => Some(e),
            _ => None,
        }
    }
}

// --- Conversions for error handling ---

impl From<io::Error> for IoError {
    fn from(e: io::Error) -> Self {
        IoError::Io(e)
    }
}

impl From<FromUtf8Error> for IoError {
    fn from(e: FromUtf8Error) -> Self {
        IoError::MinarrowError(e.to_string())
    }
}

#[cfg(feature = "zstd")]
impl From<zstd::stream::EncoderError> for IoError {
    fn from(e: zstd::stream::EncoderError) -> Self {
        IoError::Compression(format!("Zstd: {e}"))
    }
}

#[cfg(feature = "zstd")]
impl From<zstd::stream::WriteError> for IoError {
    fn from(e: zstd::stream::WriteError) -> Self {
        IoError::Compression(format!("Zstd: {e}"))
    }
}

#[cfg(feature = "snappy")]
impl From<snappy::Error> for IoError {
    fn from(e: snappy::Error) -> Self {
        IoError::Compression(format!("Snappy: {e}"))
    }
}
