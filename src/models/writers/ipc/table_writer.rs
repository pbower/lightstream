// src/io/table_writer.rs

use std::io;

use tokio::fs::File;
use minarrow::{Field, Table};
use tokio::io::AsyncWrite;
use crate::models::sinks::table_sink::GTableSink;
use crate::utils::extract_dictionary_values_from_col;
use crate::{enums::IPCMessageProtocol};
use futures_util::sink::SinkExt;

use minarrow::Vec64;
use std::pin::Pin;


/// Main Table Writer
///
/// This struct provides all high-level async methods for writing Arrow tables,
/// managing schema, dictionaries, and protocol negotiation.
/// Internally delegates to a wrapped [`GTableSink`] instance.
pub struct TableWriter<W>
where
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    sink: GTableSink<W, Vec64<u8>>,
}

impl<W> TableWriter<W>
where
    W: AsyncWrite + Unpin + Send + Sync + 'static,
{
    /// Create a new generic Arrow Table writer.
    pub fn new(
        destination: W,
        schema: Vec<Field>,
        protocol: IPCMessageProtocol,
    ) -> io::Result<Self> {
        Ok(Self {
            sink: GTableSink::new(destination, schema, protocol)?,
        })
    }

    /// Get the schema used for this writer.
    pub fn schema(&self) -> &[Field] {
        &self.sink.schema
    }
        
    /// Register a dictionary with the given id and values.
    pub fn register_dictionary(&mut self, dict_id: i64, values: Vec<String>) {
        self.sink.inner.register_dictionary(dict_id, values);
    }

    /// Return the protocol in use (Stream or File).
    pub fn protocol(&self) -> IPCMessageProtocol {
        self.sink.protocol
    }

    /// Write a single table to the sink and flush all output.
    pub async fn write_table(&mut self, table: Table) -> io::Result<()> {
        SinkExt::send(&mut self.sink, table).await?;
        SinkExt::flush(&mut self.sink).await?;
        Ok(())
    }

    /// Write all tables from an iterator to the sink and flush/close.
    pub async fn write_all_tables<I>(&mut self, tables: I) -> io::Result<()>
    where
        I: IntoIterator<Item = Table>,
    {
        let mut sink = Pin::new(&mut self.sink);
        for table in tables {
            SinkExt::send(&mut sink, table).await?;
        }
        SinkExt::close(&mut sink).await?;
        Ok(())
    }

    /// Closes the sink. Must be done after writing all of the tables.
    pub async fn finish(&mut self) -> io::Result<()> {
        SinkExt::close(&mut self.sink).await
    }
}

/// Write a sequence of `Table`s to disk in Arrow File format.
///
/// * `file_path`   – where to create/write the .arrow file  
/// * `tables`      – the batches to write (each a `Table`)  
/// * `schema`      – the common schema (must match each `Table`)  
/// * `protocol`    – usually `IPCMessageProtocol::File`  
pub async fn write_tables_to_file(
    file_path: &str,
    tables: &[Table],
    schema: Vec<Field>,
) -> io::Result<()> {
    let file = File::create(file_path).await?;
    let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;
    // Automatically register any Categorical dictionaries found in the tables.
    for table in tables {
        for (col_idx, col) in table.cols.iter().enumerate() {
            if let Some(values) = extract_dictionary_values_from_col(col) {
                // We use the column index as the unique dictionary key
                writer.register_dictionary(col_idx as i64, values);
            }
        }
    }
    writer.write_all_tables(tables.to_vec()).await?;
    Ok(())
}

/// Writes a single table to a file
pub async fn write_table_to_file(
    file_path: &str,
    table: &Table,
    schema: Vec<Field>,
) -> io::Result<()> {
    let file = File::create(file_path).await?;
    let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;
    // Automatically register any Categorical dictionaries found in the table.
    for (col_idx, col) in table.cols.iter().enumerate() {
        if let Some(values) = extract_dictionary_values_from_col(col) {
            writer.register_dictionary(col_idx as i64, values);
        }
    }
    writer.write_table(table.clone()).await?;
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use minarrow::{Array, ArrowType, Bitmask, Buffer, CategoricalArray, Field, FieldArray, Table, TextArray, Vec64};
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    fn dict_strs() -> Vec<String> {
        vec!["apple".to_string(), "banana".to_string(), "pear".to_string()]
    }

    fn make_bitmask(valid: &[bool]) -> Bitmask {
        let mut bits = vec![0u8; (valid.len() + 7) / 8];
        for (i, v) in valid.iter().enumerate() {
            if *v {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        Bitmask {
            bits: Buffer::from(Vec64::from_slice(&bits[..])),
            len: valid.len()
        }
    }

    fn make_schema() -> Vec<Field> {
        vec![Field {
            name: "col".to_string(),
            dtype: ArrowType::Dictionary(minarrow::ffi::arrow_dtype::CategoricalIndexType::UInt32),
            nullable: true,
            metadata: Default::default(),
        }]
    }

    fn make_table() -> Table {
        let arr = CategoricalArray {
            data: Buffer::from(Vec64::from_slice(&[1u32, 0, 2, 1])),
            unique_values: Vec64::from(dict_strs()),
            null_mask: Some(make_bitmask(&[true, false, true, true])),
        };
        Table {
            cols: vec![FieldArray::new(
                Field {
                    name: "col".to_string(),
                    dtype: ArrowType::Dictionary(minarrow::ffi::arrow_dtype::CategoricalIndexType::UInt32),
                    nullable: true,
                    metadata: Default::default(),
                },
                Array::TextArray(TextArray::Categorical32(Arc::new(arr))),
            )],
            n_rows: 4,
            name: "tbl".to_string(),
        }
    }

    #[tokio::test]
    async fn test_table_writer_file_protocol() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let file = File::create(&path).await.unwrap();
        let schema = make_schema();
        let mut writer = TableWriter::new(file, schema.clone(), IPCMessageProtocol::File).unwrap();
        writer.register_dictionary(0, dict_strs());

        let tbl = make_table();
        // now write *and* finish in one call
        writer.write_all_tables(vec![tbl]).await.unwrap();

        // Validate: read file and check it's not empty and starts/ends with Arrow magic.
        let mut file = File::open(&path).await.unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        // Arrow "ARROW1\0\0" magic
        assert!(buf.starts_with(b"ARROW1\0\0"));
        assert!(buf.ends_with(b"ARROW1"));
    }

    #[tokio::test]
    async fn test_table_writer_stream_protocol() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let file = File::create(&path).await.unwrap();
        let schema = make_schema();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::Stream).unwrap();
        writer.register_dictionary(0, dict_strs());

        let tbl = make_table();
        writer.write_all_tables(vec![tbl]).await.unwrap();

        let mut file = File::open(&path).await.unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        // Stream protocol starts with 0xFFFF_FFFF
        assert_eq!(&buf[..4], &[0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[tokio::test]
    async fn test_finish_idempotent() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let file = File::create(&path).await.unwrap();
        let schema = make_schema();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::Stream).unwrap();
        writer.register_dictionary(0, dict_strs());

        let tbl = make_table();
        // first write+close
        writer.write_all_tables(vec![tbl]).await.unwrap();
        // second close with no new tables
        writer.write_all_tables(Vec::<Table>::new()).await.unwrap();
    }

    #[tokio::test]
    async fn test_simd_aligned_table_writer() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path().to_path_buf();

        let file = File::create(&path).await.unwrap();
        let schema = make_schema();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File).unwrap();
        writer.register_dictionary(0, dict_strs());

        let tbl = make_table();
        writer.write_all_tables(vec![tbl]).await.unwrap();

        let mut file = File::open(&path).await.unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        assert!(buf.starts_with(b"ARROW1\0\0"));
        assert!(buf.ends_with(b"ARROW1"));
    }

    #[tokio::test]
    async fn test_error_handling_invalid_sink() {
        // Use an io::sink, which always returns Ok, so this just checks for panics.
        let schema = make_schema();
        let sink = tokio::io::sink();
        let mut writer = TableWriter::new(sink, schema, IPCMessageProtocol::File).unwrap();
        // writing nothing should simply close without error
        writer.write_all_tables(Vec::<Table>::new()).await.unwrap();
    }
}
