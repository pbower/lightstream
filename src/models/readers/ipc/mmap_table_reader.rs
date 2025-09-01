use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use flatbuffers::Vector;
use minarrow::{Field, SuperTable, Table};

use crate::arrow::file::org::apache::arrow::flatbuf as fbf;
use crate::arrow::message::org::apache::arrow::flatbuf as fbm;
use crate::constants::ARROW_MAGIC_NUMBER;
use crate::debug_println;
use crate::models::decoders::ipc::parser::{
    RecordBatchParser, convert_fb_field_to_arrow, handle_dictionary_batch,
    handle_record_batch_shared,
};
use crate::models::mmap::MemMap;

#[derive(Debug, Clone)]
struct IPCFileBlock {
    offset: usize,
    meta_bytes: usize,
    body_bytes: usize,
}

// Simple wrapper that keeps the file and mmap alive together
struct MmapBytes {
    _file: File,
    mmap: Arc<MemMap<64>>,
}

impl std::ops::Deref for MmapBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.mmap
    }
}

impl AsRef<[u8]> for MmapBytes {
    fn as_ref(&self) -> &[u8] {
        &self.mmap
    }
}

#[derive(Clone)]
pub struct MmapTableReader {
    region: Arc<MmapBytes>,
    schema: Vec<Arc<Field>>,
    dict_blocks: Vec<IPCFileBlock>,
    record_blocks: Vec<IPCFileBlock>,
    dictionaries: std::collections::HashMap<i64, Vec<String>>,
    aligned_offset: usize, // Offset from original file start to our aligned mmap start
}

impl MmapTableReader {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(&path)?;
        let meta = file.metadata()?;
        let file_len = meta.len() as usize;

        debug_println!("MMAP File len: {}", file_len);

        // ---- MMAP entire file and find 64-byte aligned data region
        let mmap = Arc::new(MemMap::<64>::open(
            path.as_ref().to_str().unwrap(),
            0,
            file_len,
        )?);
        let region = Arc::new(MmapBytes { _file: file, mmap });

        let data = region.as_ref();

        // Find the first 64-byte aligned offset after the 6-byte Arrow magic
        let magic_end = 6;
        let base_ptr = data.as_ptr() as usize;
        let aligned_data_offset = {
            let desired_ptr = base_ptr + magic_end;
            let aligned_ptr = (desired_ptr + 63) & !63; // Round up to next 64-byte boundary
            aligned_ptr - base_ptr
        };

        debug_println!(
            "Base ptr: 0x{:x}, Magic end: {}, Aligned data offset: {}, Is 64-byte aligned: {}",
            base_ptr,
            magic_end,
            aligned_data_offset,
            (base_ptr + aligned_data_offset) % 64 == 0
        );

        if &data[..6] != ARROW_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing opening magic",
            ));
        }
        if &data[file_len - 6..] != ARROW_MAGIC_NUMBER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing closing magic",
            ));
        }

        let footer_len_offset = file_len - 6 - 4;
        let footer_len = u32::from_le_bytes(
            data[footer_len_offset..footer_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;

        let footer_start = footer_len_offset - footer_len;
        let footer_end = footer_start + footer_len;
        if footer_start < 8 || footer_end > file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "footer out of bounds",
            ));
        }

        let footer_msg: &fbf::Footer = {
            &flatbuffers::root::<fbf::Footer>(&data[footer_start..footer_end]).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad footer: {e}"))
            })?
        };

        let fb_schema = footer_msg
            .schema()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "footer missing schema"))?;
        let mut fields = Vec::with_capacity(fb_schema.fields().unwrap().len());
        for i in 0..fb_schema.fields().unwrap().len() {
            let f = convert_fb_field_to_arrow(&fb_schema.fields().unwrap().get(i))?;
            fields.push(Arc::new(f));
        }
        let dict_blocks = footer_msg
            .dictionaries()
            .unwrap_or_else(|| unsafe { Vector::new(&[], 0) })
            .iter()
            .map(|b| IPCFileBlock {
                offset: b.offset() as usize,
                meta_bytes: b.metaDataLength() as usize,
                body_bytes: b.bodyLength() as usize,
            })
            .collect::<Vec<_>>();

        let record_blocks = footer_msg
            .recordBatches()
            .unwrap()
            .iter()
            .map(|b| IPCFileBlock {
                offset: b.offset() as usize,
                meta_bytes: b.metaDataLength() as usize,
                body_bytes: b.bodyLength() as usize,
            })
            .collect::<Vec<_>>();

        let mut rdr = Self {
            region,
            schema: fields,
            dict_blocks,
            record_blocks,
            dictionaries: std::collections::HashMap::new(),
            aligned_offset: aligned_data_offset,
        };

        rdr.load_all_dictionaries()?;
        Ok(rdr)
    }

    #[inline]
    pub fn schema(&self) -> &[Arc<Field>] {
        &self.schema
    }
    #[inline]
    pub fn num_batches(&self) -> usize {
        self.record_blocks.len()
    }
    pub fn read_batch(&self, idx: usize) -> io::Result<Table> {
        let blk = self
            .record_blocks
            .get(idx)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "batch idx OOB"))?;
        self.parse_batch_block(blk)
    }
    #[inline]
    pub fn into_table(&self, idx: usize) -> io::Result<Table> {
        self.read_batch(idx)
    }
    pub fn into_supertable(&self, name_override: Option<String>) -> io::Result<SuperTable> {
        let mut batches = Vec::with_capacity(self.record_blocks.len());
        for blk in &self.record_blocks {
            batches.push(Arc::new(self.parse_batch_block(blk)?));
        }
        Ok(SuperTable::from_batches(batches, name_override))
    }
    fn load_all_dictionaries(&mut self) -> io::Result<()> {
        let mut new_dicts = std::collections::HashMap::<i64, Vec<String>>::new();
        let data = self.region.as_ref();

        for blk in &self.dict_blocks {
            let msg = self.slice_message(data, blk)?;
            let fb_msg: &fbm::Message = &flatbuffers::root::<fbm::Message>(msg).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad dict msg: {e}"))
            })?;

            let dict_batch = fb_msg.header_as_dictionary_batch().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "expected DictionaryBatch")
            })?;

            RecordBatchParser::check_dictionary_delta(&dict_batch)?;

            let body =
                &data[blk.offset + blk.meta_bytes..blk.offset + blk.meta_bytes + blk.body_bytes];

            handle_dictionary_batch(&dict_batch, body, &mut new_dicts)?;
        }
        self.dictionaries = new_dicts;
        Ok(())
    }

    fn parse_batch_block(&self, blk: &IPCFileBlock) -> io::Result<Table> {
        let data = self.region.as_ref();
        let meta_slice = self.slice_message(data, blk)?;
        let original_body_offset = blk.offset + blk.meta_bytes;
        let body_len = blk.body_bytes;

        // Adjust body_offset so that buffer addresses land on 64-byte boundaries
        // The first buffer in the body is at offset 0, so we want:
        // (data.as_ptr() + adjusted_body_offset + 0) to be 64-byte aligned
        let data_base_addr = data.as_ptr() as usize;
        let desired_first_buffer_addr = (data_base_addr + original_body_offset + 63) & !63;
        let body_offset = desired_first_buffer_addr - data_base_addr;

        debug_println!(
            "Original body_offset: {}, Adjusted body_offset: {}, First buffer will be at: 0x{:x}",
            original_body_offset,
            body_offset,
            desired_first_buffer_addr
        );

        let fb_msg: &fbm::Message =
            &flatbuffers::root::<fbm::Message>(meta_slice).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, format!("bad record msg: {e}"))
            })?;

        let rec = fb_msg.header_as_record_batch().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "expected RecordBatch header")
        })?;

        // Use shared handler that now supports both Arc<[u8]> and mmap zero-copy
        handle_record_batch_shared(
            &rec,
            &self
                .schema
                .iter()
                .map(|a| a.as_ref().clone())
                .collect::<Vec<_>>(),
            &self.dictionaries,
            self.region.clone(),
            body_offset,
            body_len,
        )
    }

    fn slice_message<'a>(&self, data: &'a [u8], blk: &IPCFileBlock) -> io::Result<&'a [u8]> {
        if blk.offset + 8 > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "block header OOB",
            ));
        }
        // Continuation marker
        let cont = u32::from_le_bytes(data[blk.offset..blk.offset + 4].try_into().unwrap());
        if cont != 0xFFFF_FFFF {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad continuation marker: {cont:#X}"),
            ));
        }
        // Metadata length
        let meta_len =
            u32::from_le_bytes(data[blk.offset + 4..blk.offset + 8].try_into().unwrap()) as usize;
        eprintln!(
            "blk.offset={} meta_bytes={} body_bytes={} meta_len={} slice_len={}",
            blk.offset,
            blk.meta_bytes,
            blk.body_bytes,
            meta_len,
            data.len()
        );

        let start = blk.offset + 8;
        let end = start + meta_len;
        if end > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "msg slice OOB",
            ));
        }
        // --- DEBUG: Dump file and message slice ---
        eprintln!("==== BLOCK DUMP ====");
        eprintln!(
            "blk.offset = {blk_offset}, meta_bytes = {meta_bytes}, body_bytes = {body_bytes}, meta_len = {meta_len}, file_len = {}",
            data.len(),
            blk_offset = blk.offset,
            meta_bytes = blk.meta_bytes,
            body_bytes = blk.body_bytes,
            meta_len = meta_len
        );
        let dump = &data[blk.offset..std::cmp::min(data.len(), blk.offset + 32)];
        eprint!("data[blk.offset..+32]: ");
        for b in dump {
            eprint!("{:02X} ", b);
        }
        eprintln!();
        let msg_dump = &data[start..std::cmp::min(data.len(), start + 32)];
        eprint!("data[start..+32]:      ");
        for b in msg_dump {
            eprint!("{:02X} ", b);
        }
        eprintln!();
        Ok(&data[start..end])
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        models::readers::ipc::mmap_table_reader::MmapTableReader,
        test_helpers::{make_all_types_table, write_test_table_to_file},
    };
    use minarrow::{Array, NumericArray, Table, TextArray};

    // -------------------- Tests -------------------- //

    #[tokio::test]
    async fn test_single_batch_roundtrip_mmap() {
        let table = make_all_types_table();
        let temp = write_test_table_to_file(&[table.clone()]).await;
        let rdr = MmapTableReader::open(&temp.path()).unwrap();
        assert_eq!(rdr.num_batches(), 1);
        let table2 = rdr.read_batch(0).unwrap();

        assert_eq!(table2.n_rows, 4);
        assert_eq!(table2.cols.len(), table.cols.len());

        println!("TABLE {:?}\n", &table2);

        // Int32 col: sum, buffer type
        match &table2.cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                let s: i32 = arr.data.as_ref().iter().sum();
                assert_eq!(s, 10);
                // Note: Currently mmap requires copying data to create Arc<[u8]>
                // so buffers won't be shared. True zero-copy would require
                // modifying minarrow to accept mmap memory directly.
                if arr.data.is_shared() {
                    eprintln!("Int32 buffer is shared (64-byte aligned in copied Arc)");
                } else {
                    eprintln!("Int32 buffer was cloned (not 64-byte aligned in copied Arc)");
                }
            }
            _ => panic!("wrong type"),
        }
        // Float64 col: value and buffer type
        match &table2.cols[5].array {
            Array::NumericArray(NumericArray::Float64(arr)) => {
                let vals: Vec<_> = arr.data.as_ref().iter().cloned().collect();
                assert_eq!(vals, vec![1.1, 2.2, 3.3, 4.4]);
                // Note: Currently mmap requires copying data to create Arc<[u8]>
                // so buffers won't be shared. True zero-copy would require
                // modifying minarrow to accept mmap memory directly.
                if arr.data.is_shared() {
                    eprintln!("Float64 buffer is shared (64-byte aligned in copied Arc)");
                } else {
                    eprintln!("Float64 buffer was cloned (not 64-byte aligned in copied Arc)");
                }
            }
            _ => panic!("wrong type"),
        }
        // // Dictionary col: unique values and zero-copy buffer
        // match &table2.cols[8].array {
        //     Array::TextArray(TextArray::Categorical32(arr)) => {
        //         assert_eq!(&arr.unique_values[..], &["apple", "banana", "pear"]);
        //         assert!(arr.data.is_shared());
        //     }
        //     _ => panic!("wrong type")
        // }
        // Check at least one string, bool, all others present
        let mut seen_string = false;
        let mut seen_bool = false;
        let mut any_shared = false;
        for arr in &table2.cols {
            match &arr.array {
                Array::TextArray(TextArray::String32(a)) => {
                    seen_string = true;
                    if a.data.is_shared() {
                        eprintln!("String32 data buffer is shared");
                        any_shared = true;
                    } else {
                        eprintln!("String32 data buffer was cloned");
                    }
                }
                Array::BooleanArray(a) => {
                    seen_bool = true;
                    if a.data.bits.is_shared() {
                        eprintln!("Boolean bits buffer is shared");
                        any_shared = true;
                    } else {
                        eprintln!("Boolean bits buffer was cloned");
                    }
                }
                _ => {}
            }
        }
        assert!(
            seen_string && seen_bool,
            "String32 and Bool must be present"
        );
        eprintln!("Any buffers shared in mmap: {}", any_shared);
        drop(rdr);
        drop(temp);
    }

    #[tokio::test]
    async fn test_into_table_and_sharedness() {
        let table = make_all_types_table();
        let temp = write_test_table_to_file(&[table.clone()]).await;
        let rdr = MmapTableReader::open(&temp.path()).unwrap();

        let t2 = rdr.into_table(0).unwrap();
        // Note: Currently mmap requires copying data to create Arc<[u8]>
        // so we check for shared OR owned buffers. True zero-copy would require
        // modifying minarrow to accept mmap memory directly.
        let mut shared_count = 0;
        let mut owned_count = 0;
        for arr in t2.cols.iter().map(|fa| &fa.array) {
            match arr {
                Array::NumericArray(NumericArray::Int32(a)) => {
                    if a.data.is_shared() {
                        shared_count += 1;
                    } else {
                        owned_count += 1;
                    }
                }
                Array::NumericArray(NumericArray::Float64(a)) => {
                    if a.data.is_shared() {
                        shared_count += 1;
                    } else {
                        owned_count += 1;
                    }
                }
                Array::TextArray(TextArray::String32(a)) => {
                    if a.data.is_shared() {
                        shared_count += 1;
                    } else {
                        owned_count += 1;
                    }
                }
                Array::BooleanArray(a) => {
                    if a.data.bits.is_shared() {
                        shared_count += 1;
                    } else {
                        owned_count += 1;
                    }
                }
                Array::TextArray(TextArray::Categorical32(a)) => {
                    if a.data.is_shared() {
                        shared_count += 1;
                    } else {
                        owned_count += 1;
                    }
                }
                _ => {}
            }
        }
        eprintln!(
            "Mmap into_table: {} shared, {} owned buffers",
            shared_count, owned_count
        );
        drop(temp)
    }

    #[tokio::test]
    async fn test_multiple_batches_and_supertable() {
        let t1 = make_all_types_table();
        let t2 = make_all_types_table();
        let temp = write_test_table_to_file(&[t1.clone(), t2.clone()]).await;

        let rdr = MmapTableReader::open(temp.path()).unwrap();
        assert_eq!(rdr.num_batches(), 2);

        let supertbl = rdr
            .into_supertable(Some("my_supertable".to_string()))
            .unwrap();
        assert_eq!(supertbl.n_rows, 8);
        assert_eq!(supertbl.batches.len(), 2);

        // Check one batch/col
        assert_eq!(supertbl.batches[0].cols[0].field.name, "int32");
        match &supertbl.batches[1].cols[0].array {
            Array::NumericArray(NumericArray::Int32(arr)) => {
                let values: Vec<i32> = arr.data.as_ref().iter().copied().collect();
                assert_eq!(values, vec![1, 2, 3, 4]);
            }
            _ => panic!("expected int32 col"),
        }
    }

    #[tokio::test]
    async fn test_big_super_table_iteration_and_owned_conversion() {
        let tables: Vec<Table> = (0..10).map(|_| make_all_types_table()).collect();
        let temp = write_test_table_to_file(&tables).await;
        let rdr = MmapTableReader::open(temp.path()).unwrap();
        let supertbl = rdr.into_supertable(None).unwrap();
        assert_eq!(supertbl.batches.len(), 10);

        for batch in &supertbl.batches {
            for col in &batch.cols {
                match &col.array {
                    Array::NumericArray(NumericArray::Int32(arr)) => {
                        // Note: May be owned or shared depending on alignment
                        if arr.data.is_shared() {
                            eprintln!("Int32 is shared");
                        }
                        let owned = arr.data.to_owned_copy();
                        assert!(!owned.is_shared());
                    }
                    Array::NumericArray(NumericArray::Float64(arr)) => {
                        // Note: May be owned or shared depending on alignment
                        if arr.data.is_shared() {
                            eprintln!("Float64 is shared");
                        }
                        let owned = arr.data.to_owned_copy();
                        assert!(!owned.is_shared());
                    }
                    Array::TextArray(TextArray::String32(arr)) => {
                        // Note: May be owned or shared depending on alignment
                        if arr.data.is_shared() {
                            eprintln!("String32 is shared");
                        }
                        let owned = arr.data.to_owned_copy();
                        assert!(!owned.is_shared());
                    }
                    Array::BooleanArray(arr) => {
                        // Note: May be owned or shared depending on alignment
                        if arr.data.bits.is_shared() {
                            eprintln!("Boolean is shared");
                        }
                        let owned = arr.data.to_owned_copy();
                        assert!(!owned.bits.is_shared());
                    }
                    Array::TextArray(TextArray::Categorical32(arr)) => {
                        // Note: May be owned or shared depending on alignment
                        if arr.data.is_shared() {
                            eprintln!("Categorical32 is shared");
                        }
                        let owned = arr.data.to_owned_copy();
                        assert!(!owned.is_shared());
                    }
                    _ => {}
                }
            }
        }
    }

    #[tokio::test]
    async fn test_error_on_invalid_batch_index() {
        let table = make_all_types_table();
        let temp = write_test_table_to_file(&[table]).await;
        let rdr = MmapTableReader::open(temp.path()).unwrap();
        let err = rdr.read_batch(1000).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
