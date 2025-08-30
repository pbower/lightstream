//! Parquet file, schema and page-metadata serialisation.
use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write};

use crate::error::IoError;
use crate::models::types::parquet::{ParquetEncoding, ParquetPhysicalType};

/// Required “PAR1” marker written at head and tail.
pub const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

// --------------------- Structs ------------------------------------ //
/// Complete file metadata for footer.
#[derive(Debug, Clone)]
pub(crate) struct FileMetaData {
    pub version: i32,
    pub schema: Vec<SchemaElement>,
    pub num_rows: i64,
    pub row_groups: Vec<RowGroupMeta>,
    pub key_value_metadata: Option<BTreeMap<String, String>>,
    pub created_by: Option<String>,
}

/// Types as SchemaElements.
#[derive(Debug, Clone)]
pub(crate) struct SchemaElement {
    pub name: String,
    pub repetition_type: i32, // 0 = REQUIRED, 1 = OPTIONAL, 2 = REPEATED
    pub type_: Option<ParquetPhysicalType>,
    pub converted_type: Option<i32>, // legacy logical-type id
    pub type_length: Option<i32>,
    pub precision: Option<i32>,
    pub scale: Option<i32>,
    pub field_id: Option<i32>,
}

/// Row group metadata.
#[derive(Debug, Clone)]
pub(crate) struct RowGroupMeta {
    pub columns: Vec<ColumnChunkMeta>,
    pub total_byte_size: i64,
    pub num_rows: i64,
}

/// Each column chunk, for primitive, unsigned, or categorical/dictionary columns.
#[derive(Debug, Clone)]
pub(crate) struct ColumnChunkMeta {
    pub file_offset: i64,
    pub meta_data: ColumnMetadata,
}

/// For categorical/dictionary types.
#[derive(Debug, Clone)]
pub(crate) struct ColumnMetadata {
    pub type_: ParquetPhysicalType,
    pub encodings: Vec<ParquetEncoding>,
    pub path_in_schema: Vec<String>,
    pub codec: i32,
    pub num_values: i64,
    pub total_uncompressed_size: i64,
    pub total_compressed_size: i64,
    /// Byte offset from the start of the file to the first data page
    /// of this column chunk.
    pub data_page_offset: i64,
    /// Optional byte offset from the start of the file to the dictionary page
    /// for this column chunk.  `None` if no dictionary page was written.
    pub dictionary_page_offset: Option<i64>,
    pub statistics: Option<Statistics>,
    pub definition_level: i32,
}

/// Parquet format includes statistics
///
/// => Min, max, null/unique count
#[derive(Debug, Clone)]
pub(crate) struct Statistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
}

/// Parquet DataPageHeader (from parquet.thrift).
#[derive(Debug, Clone)]
pub(crate) struct DataPageHeader {
    pub num_values: i32,
    pub encoding: ParquetEncoding,
    pub definition_level_encoding: ParquetEncoding,
    pub repetition_level_encoding: ParquetEncoding,
    pub statistics: Option<Statistics>,
}

/// Parquet DataPageHeaderV2
///
/// Introduced in parquet v2 (via thrift)
#[derive(Debug, Clone)]
pub(crate) struct DataPageHeaderV2 {
    pub num_rows: i32,
    pub num_nulls: i32,
    pub num_values: i32,
    pub encoding: ParquetEncoding,
    pub definition_levels_byte_length: i32,
    pub repetition_levels_byte_length: i32,
    pub is_compressed: bool,
    pub statistics: Option<Statistics>,
}

#[derive(Debug, Clone)]
pub(crate) struct PageHeader {
    pub type_: PageType,
    pub uncompressed_page_size: i32,
    pub compressed_page_size: i32,
    pub data_page_header: Option<DataPageHeader>,
    pub data_page_header_v2: Option<DataPageHeaderV2>,
    pub dictionary_page_header: Option<DictionaryPageHeader>,
}

/// Parquet Dictionary Page Header, as required for categorical/dictionary columns.
#[derive(Debug, Clone)]
pub(crate) struct DictionaryPageHeader {
    pub num_values: i32,
    pub encoding: ParquetEncoding,
    pub is_sorted: Option<bool>,
}

// --------------------- Enums ------------------------------------ //

/// Parquet Page types (from parquet.thrift).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageType {
    DataPage = 0,
    IndexPage = 1,
    DictionaryPage = 2,
    DataPageV2 = 3,
}

// --------------------- Implementations --------------------------- //

impl PageType {
    pub fn as_i32(self) -> i32 {
        self as i32
    }
    pub fn from_i32(v: i32) -> Option<Self> {
        Some(match v {
            0 => Self::DataPage,
            1 => Self::IndexPage,
            2 => Self::DictionaryPage,
            3 => Self::DataPageV2,
            _ => return None,
        })
    }
}

impl DataPageHeader {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] num_values
        thrift_write_field_i32(&mut w, 1, self.num_values);

        // [2] encoding
        thrift_write_field_i32(&mut w, 2, self.encoding.to_i32());

        // [3] definition_level_encoding
        thrift_write_field_i32(&mut w, 3, self.definition_level_encoding.to_i32());

        // [4] repetition_level_encoding
        thrift_write_field_i32(&mut w, 4, self.repetition_level_encoding.to_i32());

        // [5] statistics (optional)
        if let Some(ref stats) = self.statistics {
            thrift_write_field_struct_begin(&mut w, 5);
            stats.write(&mut w)?;
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl DataPageHeaderV2 {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);
        thrift_write_field_i32(&mut w, 1, self.num_rows);
        thrift_write_field_i32(&mut w, 2, self.num_nulls);
        thrift_write_field_i32(&mut w, 3, self.num_values);
        thrift_write_field_i32(&mut w, 4, self.encoding.to_i32());
        thrift_write_field_i32(&mut w, 5, self.definition_levels_byte_length);
        thrift_write_field_i32(&mut w, 6, self.repetition_levels_byte_length);
        thrift_write_field_bool(&mut w, 7, self.is_compressed);
        if let Some(ref s) = self.statistics {
            thrift_write_field_struct_begin(&mut w, 8);
            s.write(&mut w)?;
        }
        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl PageHeader {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);
        thrift_write_field_i32(&mut w, 1, self.type_.as_i32());
        thrift_write_field_i32(&mut w, 2, self.uncompressed_page_size);
        thrift_write_field_i32(&mut w, 3, self.compressed_page_size);

        if let Some(ref h) = self.data_page_header {
            thrift_write_field_struct_begin(&mut w, 4);
            h.write(&mut w)?;
        }
        if let Some(ref h2) = self.data_page_header_v2 {
            thrift_write_field_struct_begin(&mut w, 7); // field id 7 for V2
            h2.write(&mut w)?;
        }
        if let Some(ref d) = self.dictionary_page_header {
            thrift_write_field_struct_begin(&mut w, 5);
            d.write(&mut w)?;
        }
        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl DictionaryPageHeader {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] num_values: i32
        thrift_write_field_i32(&mut w, 1, self.num_values);

        // [2] encoding: i32
        thrift_write_field_i32(&mut w, 2, self.encoding.to_i32());

        // [3] is_sorted: optional bool
        if let Some(val) = self.is_sorted {
            thrift_write_field_bool(&mut w, 3, val);
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl FileMetaData {
    /// Serialise the footer and write tail magic marker.  
    /// Caller must have written the file body already.
    pub fn write<W: Write + Seek>(&self, mut w: W) -> Result<u64, IoError> {
        let start_pos = w.seek(SeekFrom::Current(0))?;

        //  Thrift encode FileMetaData struct into a buffer
        let mut buf = Vec::new();
        thrift_write_struct_begin(&mut buf, 0x0c);

        thrift_write_field_i32(&mut buf, 1, self.version);

        thrift_write_field_list_begin(&mut buf, 2, 12, self.schema.len() as i32);
        for s in &self.schema {
            s.write(&mut buf)?;
        }

        thrift_write_field_i64(&mut buf, 3, self.num_rows);

        thrift_write_field_list_begin(&mut buf, 4, 12, self.row_groups.len() as i32);
        for rg in &self.row_groups {
            rg.write(&mut buf)?;
        }

        if let Some(ref kv) = self.key_value_metadata {
            thrift_write_field_map_begin(&mut buf, 5, 11, 11, kv.len() as i32);
            for (k, v) in kv {
                thrift_write_string(&mut buf, k);
                thrift_write_string(&mut buf, v);
            }
        }

        if let Some(ref s) = self.created_by {
            thrift_write_field_string(&mut buf, 6, s);
        }

        thrift_write_field_stop(&mut buf);
        println!("DIAG: FileMetaData encoded len = {}", buf.len());
        // footer + length + magic
        w.write_all(&buf)?;
        let footer_len = buf.len() as u32;
        w.write_all(&footer_len.to_le_bytes())?; // 32-bit little-endian length
        w.write_all(PARQUET_MAGIC)?; // tail magic

        Ok(start_pos)
    }
}

// SchemaElement

impl SchemaElement {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        thrift_write_field_string(&mut w, 1, &self.name);

        if let Some(ty) = self.type_ {
            thrift_write_field_i32(&mut w, 2, ty.as_i32());
        }

        thrift_write_field_i32(&mut w, 3, self.repetition_type);

        // legacy converted_type for UTF-8 text
        match self.converted_type {
            Some(ct) => thrift_write_field_i32(&mut w, 6, ct),
            None if matches!(self.type_, Some(ParquetPhysicalType::ByteArray)) => {
                thrift_write_field_i32(&mut w, 6, 1); // 1 = UTF8
            }
            _ => {}
        }

        if let Some(len) = self.type_length {
            thrift_write_field_i32(&mut w, 7, len);
        }
        if let Some(p) = self.precision {
            thrift_write_field_i32(&mut w, 9, p);
        }
        if let Some(s) = self.scale {
            thrift_write_field_i32(&mut w, 10, s);
        }
        if let Some(id) = self.field_id {
            thrift_write_field_i32(&mut w, 15, id);
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl RowGroupMeta {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] columns: list<ColumnChunkMeta>
        thrift_write_field_list_begin(&mut w, 1, 12, self.columns.len() as i32);
        for col in &self.columns {
            col.write(&mut w)?;
        }

        // [2] total_byte_size: i64
        thrift_write_field_i64(&mut w, 2, self.total_byte_size);

        // [3] num_rows: i64
        thrift_write_field_i64(&mut w, 3, self.num_rows);

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl ColumnChunkMeta {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] file_offset: i64
        thrift_write_field_i64(&mut w, 1, self.file_offset);

        // [2] meta_data: ColumnMetaData
        thrift_write_field_struct_begin(&mut w, 2);
        self.meta_data.write(&mut w)?;

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl ColumnMetadata {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        // [1] type: INT32/INT64/… as i32
        thrift_write_field_i32(&mut w, 1, self.type_.as_i32());

        // [2] encodings: list<i32>
        thrift_write_field_list_begin(&mut w, 2, 8, self.encodings.len() as i32);
        for &e in &self.encodings {
            w.write_all(&e.to_i32().to_le_bytes())?;
        }

        // [3] path_in_schema: list<string>
        thrift_write_field_list_begin(&mut w, 3, 11, self.path_in_schema.len() as i32);
        for s in &self.path_in_schema {
            thrift_write_string(&mut w, s);
        }

        // [4] codec: i32
        thrift_write_field_i32(&mut w, 4, self.codec);

        // [5] num_values: i64
        thrift_write_field_i64(&mut w, 5, self.num_values);

        // [6] total_uncompressed_size: i64
        thrift_write_field_i64(&mut w, 6, self.total_uncompressed_size);

        // [7] total_compressed_size: i64
        thrift_write_field_i64(&mut w, 7, self.total_compressed_size);

        // [8] data_page_offset: i64
        //    Actual byte offset of the first data page for this column chunk.
        thrift_write_field_i64(&mut w, 8, self.data_page_offset);

        // [9] dictionary_page_offset: optional i64
        //    Actual byte offset of the dictionary page, if any.
        if let Some(dict_off) = self.dictionary_page_offset {
            thrift_write_field_i64(&mut w, 9, dict_off);
        }

        // [10] statistics: optional struct
        if let Some(ref stats) = self.statistics {
            thrift_write_field_struct_begin(&mut w, 10);
            stats.write(&mut w)?;
        }

        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

impl Statistics {
    pub fn write<W: Write>(&self, mut w: W) -> Result<(), IoError> {
        thrift_write_struct_begin(&mut w, 0x0c);

        if let Some(n) = self.null_count {
            thrift_write_field_i64(&mut w, 1, n);
        }
        if let Some(d) = self.distinct_count {
            thrift_write_field_i64(&mut w, 2, d);
        }
        if let Some(ref min) = self.min {
            thrift_write_field_bytes(&mut w, 3, min);
        }
        if let Some(ref max) = self.max {
            thrift_write_field_bytes(&mut w, 4, max);
        }
        thrift_write_field_stop(&mut w);
        Ok(())
    }
}

// --------------- Thrift serialisation helpers --------------- //

fn thrift_write_struct_begin<W: Write>(_w: &mut W, _id: u8) {}
fn thrift_write_field_stop<W: Write>(w: &mut W) {
    w.write_all(&[0]).unwrap();
}

fn thrift_write_field_i32<W: Write>(w: &mut W, id: i16, v: i32) {
    w.write_all(&[8]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&v.to_le_bytes()).unwrap();
}
fn thrift_write_field_i64<W: Write>(w: &mut W, id: i16, v: i64) {
    w.write_all(&[10]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&v.to_le_bytes()).unwrap();
}
fn thrift_write_field_bool<W: Write>(w: &mut W, id: i16, v: bool) {
    w.write_all(&[2]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&[v as u8]).unwrap();
}
fn thrift_write_field_string<W: Write>(w: &mut W, id: i16, s: &str) {
    w.write_all(&[11]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    thrift_write_string(w, s);
}
fn thrift_write_field_bytes<W: Write>(w: &mut W, id: i16, b: &[u8]) {
    w.write_all(&[11]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    let l = b.len() as i32;
    w.write_all(&l.to_le_bytes()).unwrap();
    w.write_all(b).unwrap();
}
fn thrift_write_field_list_begin<W: Write>(w: &mut W, id: i16, tpe: u8, len: i32) {
    w.write_all(&[15]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&[tpe]).unwrap();
    w.write_all(&len.to_le_bytes()).unwrap();
}
fn thrift_write_field_map_begin<W: Write>(w: &mut W, id: i16, kt: u8, vt: u8, len: i32) {
    w.write_all(&[13]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
    w.write_all(&[kt, vt]).unwrap();
    w.write_all(&len.to_le_bytes()).unwrap();
}
fn thrift_write_field_struct_begin<W: Write>(w: &mut W, id: i16) {
    w.write_all(&[12]).unwrap();
    w.write_all(&id.to_le_bytes()).unwrap();
}
fn thrift_write_string<W: Write>(w: &mut W, s: &str) {
    let len = s.len() as i32;
    w.write_all(&len.to_le_bytes()).unwrap();
    w.write_all(s.as_bytes()).unwrap();
}
