#[cfg(test)]
mod integration {
    use std::sync::Arc;

    use ::lightstream_io::enums::BufferChunkSize;
    use ::lightstream_io::enums::IPCMessageProtocol;
    use ::lightstream_io::models::readers::ipc::table_stream_reader::TableStreamReader64;
    use ::lightstream_io::models::readers::ipc::file_table_reader::FileTableReader;
    use ::lightstream_io::models::streams::disk::DiskByteStream;
    use ::lightstream_io::models::writers::ipc::table_stream_writer::TableStreamWriter;
    use ::lightstream_io::models::writers::ipc::table_writer::TableWriter;
    use futures_util::stream::StreamExt;
    use minarrow::ffi::arrow_dtype::{ArrowType, CategoricalIndexType};
    use minarrow::*;

    pub fn vec64_to_vec(v: Vec64<String>) -> Vec<String> {
        v.into_iter().collect()
    }

    pub fn make_alternating_mask(len: usize) -> minarrow::Bitmask {
        let mut bm = minarrow::Bitmask::new_set_all(len, true);
        for i in (1..len).step_by(2) {
            bm.set_false(i);
        }
        bm
    }

    fn build_all_types_table(n: usize) -> Table {
        let int32 = NumericArray::Int32(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(1..=n as i32).collect::<Vec<_>>()),
            None,
        )));
        let int64 = NumericArray::Int64(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(100..(100 + n as i64)).collect::<Vec<_>>()),
            None,
        )));
        let uint32 = NumericArray::UInt32(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(0..n as u32).collect::<Vec<_>>()),
            None,
        )));
        let uint64 = NumericArray::UInt64(Arc::new(IntegerArray::from_vec64(
            Vec64::from_slice(&(10..(10 + n as u64)).collect::<Vec<_>>()),
            None,
        )));
        let float32 = NumericArray::Float32(Arc::new(FloatArray::from_vec64(
            Vec64::from_slice(&(0..n).map(|i| i as f32 * 1.25 - 2.5).collect::<Vec<_>>()),
            None,
        )));
        let float64 = NumericArray::Float64(Arc::new(FloatArray::from_vec64(
            Vec64::from_slice(&(0..n).map(|i| i as f64 * 3.5 - 1.0).collect::<Vec<_>>()),
            None,
        )));
        let bools = BooleanArray::from_slice(&(0..n).map(|i| i % 2 == 0).collect::<Vec<_>>());

        let strs: Vec<String> = (0..n).map(|i| format!("str{i}")).collect();
        let str_refs: Vec<&str> = strs.iter().map(|s| &**s).collect();
        let string32 = StringArray::from_vec(str_refs, None);

        #[cfg(feature = "large_string")]
        let large_string = {
            let ls_refs: Vec<&str> = strs.iter().map(|s| &**s).collect();
            StringArray::from_vec(ls_refs, None)
        };

        let cat_keys: Vec64<u32> =
            Vec64::from_slice(&(0..n as u32).map(|i| i % 3).collect::<Vec<_>>());
        let cat_values: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let cat32 = CategoricalArray::new(
            minarrow::Buffer::from(cat_keys),
            Vec64::from(cat_values.clone()),
            None,
        );

        #[cfg(feature = "extended_categorical")]
        let cat_u8 = CategoricalArray::new(
            minarrow::Buffer::from(Vec64::from_slice(
                &(0..n as u8).map(|i| i % 3).collect::<Vec<_>>(),
            )),
            Vec64::from(vec!["x".to_string(), "y".to_string(), "z".to_string()]),
            None,
        );
        #[cfg(feature = "extended_categorical")]
        let cat_u16 = CategoricalArray::new(
            minarrow::Buffer::from(Vec64::from_slice(
                &(0..n as u16)
                    .map(|i| (2 - (i % 3)) as u16)
                    .collect::<Vec<_>>(),
            )),
            Vec64::from(vec![
                "foo".to_string(),
                "bar".to_string(),
                "baz".to_string(),
            ]),
            None,
        );
        #[cfg(feature = "extended_categorical")]
        let cat_u64 = CategoricalArray::new(
            minarrow::Buffer::from(Vec64::from_slice(
                &(0..n as u64).map(|i| (i % 3) as u64).collect::<Vec<_>>(),
            )),
            Vec64::from(vec!["m".to_string(), "n".to_string(), "o".to_string()]),
            None,
        );

        let mut cols = vec![
            FieldArray::new(
                Field::new("int32", ArrowType::Int32, false, None),
                Array::NumericArray(int32),
            ),
            FieldArray::new(
                Field::new("int64", ArrowType::Int64, false, None),
                Array::NumericArray(int64),
            ),
            FieldArray::new(
                Field::new("uint32", ArrowType::UInt32, false, None),
                Array::NumericArray(uint32),
            ),
            FieldArray::new(
                Field::new("uint64", ArrowType::UInt64, false, None),
                Array::NumericArray(uint64),
            ),
            FieldArray::new(
                Field::new("float32", ArrowType::Float32, false, None),
                Array::NumericArray(float32),
            ),
            FieldArray::new(
                Field::new("float64", ArrowType::Float64, false, None),
                Array::NumericArray(float64),
            ),
            FieldArray::new(
                Field::new("bool", ArrowType::Boolean, false, None),
                Array::BooleanArray(Arc::new(bools)),
            ),
            FieldArray::new(
                Field::new("string", ArrowType::String, false, None),
                Array::TextArray(TextArray::String32(Arc::new(string32))),
            ),
            FieldArray::new(
                Field::new(
                    "cat32",
                    ArrowType::Dictionary(CategoricalIndexType::UInt32),
                    false,
                    None,
                ),
                Array::TextArray(TextArray::Categorical32(Arc::new(cat32))),
            ),
        ];
        #[cfg(feature = "large_string")]
        cols.push(FieldArray::new(
            Field::new("large_string", ArrowType::LargeString, false, None),
            Array::TextArray(TextArray::String64(Arc::new(large_string))),
        ));
        #[cfg(feature = "extended_categorical")]
        {
            cols.push(FieldArray::new(
                Field::new(
                    "cat_u8",
                    ArrowType::Dictionary(CategoricalIndexType::UInt8),
                    false,
                    None,
                ),
                Array::TextArray(TextArray::Categorical8(Arc::new(cat_u8))),
            ));
            cols.push(FieldArray::new(
                Field::new(
                    "cat_u16",
                    ArrowType::Dictionary(CategoricalIndexType::UInt16),
                    false,
                    None,
                ),
                Array::TextArray(TextArray::Categorical16(Arc::new(cat_u16))),
            ));
            cols.push(FieldArray::new(
                Field::new(
                    "cat_u64",
                    ArrowType::Dictionary(CategoricalIndexType::UInt64),
                    false,
                    None,
                ),
                Array::TextArray(TextArray::Categorical64(Arc::new(cat_u64))),
            ));
        }
        Table {
            cols,
            n_rows: n,
            name: "test".to_owned(),
        }
    }

    fn dicts_for_table(table: &Table) -> Vec<(i64, Vec64<String>)> {
        table
            .cols
            .iter()
            .enumerate()
            .filter_map(|(i, col)| match &col.array {
                Array::TextArray(TextArray::Categorical32(arr)) => {
                    Some((i as i64, arr.unique_values.clone()))
                }
                #[cfg(feature = "extended_categorical")]
                Array::TextArray(TextArray::Categorical8(arr)) => {
                    Some((i as i64, arr.unique_values.clone()))
                }
                #[cfg(feature = "extended_categorical")]
                Array::TextArray(TextArray::Categorical16(arr)) => {
                    Some((i as i64, arr.unique_values.clone()))
                }
                #[cfg(feature = "extended_categorical")]
                Array::TextArray(TextArray::Categorical64(arr)) => {
                    Some((i as i64, arr.unique_values.clone()))
                }
                _ => None,
            })
            .collect()
    }

    async fn roundtrip_ipc(mode: IPCMessageProtocol, n: usize) {
        use tempfile::tempdir;
        let table = build_all_types_table(n);
        let schema: Vec<Field> = table
            .cols
            .iter()
            .map(|c| c.field.as_ref().clone())
            .collect();

        // --- Write to disk buffer
        let dir = tempdir().unwrap();
        let path = dir.path().join("arrow_roundtrip_ipc.bin");
        {
            let file = tokio::fs::File::create(&path).await.unwrap();
            let mut writer = TableWriter::new(file, schema, mode).unwrap();
            for (dict_id, unique) in dicts_for_table(&table) {
                writer.register_dictionary(dict_id, unique.to_vec());
            }
            writer.write_all_tables(vec![table.clone()]).await.unwrap();
        }

        // --- Read using appropriate reader for protocol
        let mut table2 = match mode {
            IPCMessageProtocol::File => {
                let reader = FileTableReader::open(&path).unwrap();
                reader.read_batch(0).unwrap()
            }
            IPCMessageProtocol::Stream => {
                let stream = DiskByteStream::open(&path, BufferChunkSize::Custom(128 * 1024))
                    .await
                    .unwrap();
                let mut reader = TableStreamReader64::new(stream, 128 * 1024, IPCMessageProtocol::Stream);
                reader.next().await.unwrap().unwrap()
            }
        };
        
        // Fix table name to match original for comparison
        table2.name = table.name.clone();

        // --- Data equality
        assert_eq!(table2.n_rows, table.n_rows);
        assert_eq!(table2.cols.len(), table.cols.len());
        for (a, b) in table.cols.iter().zip(table2.cols.iter()) {
            assert_eq!(a.field, b.field, "Field mismatch");
            match (&a.array, &b.array) {
                (Array::NumericArray(an), Array::NumericArray(bn)) => assert_eq!(an, bn),
                (Array::BooleanArray(an), Array::BooleanArray(bn)) => assert_eq!(an, bn),
                (Array::TextArray(at), Array::TextArray(bt)) => assert_eq!(at, bt),
                _ => panic!("Mismatched array types: {:?} vs {:?}", a.array, b.array),
            }
        }
    }

    #[tokio::test]
    async fn test_roundtrip_stream_6rows() {
        roundtrip_ipc(IPCMessageProtocol::Stream, 6).await;
    }

    #[tokio::test]
    async fn test_roundtrip_stream_1row() {
        roundtrip_ipc(IPCMessageProtocol::Stream, 1).await;
    }

    #[tokio::test]
    async fn test_roundtrip_file_6rows() {
        roundtrip_ipc(IPCMessageProtocol::File, 6).await;
    }

    #[tokio::test]
    async fn test_roundtrip_file_1row() {
        roundtrip_ipc(IPCMessageProtocol::File, 1).await;
    }

    #[tokio::test]
    async fn test_roundtrip_stream_nulls() {
        roundtrip_ipc_with_nulls(IPCMessageProtocol::Stream, 7).await;
    }

    #[tokio::test]
    async fn test_roundtrip_file_nulls() {
        roundtrip_ipc_with_nulls(IPCMessageProtocol::File, 7).await;
    }

    async fn roundtrip_ipc_with_nulls(fmt: IPCMessageProtocol, n: usize) {
        use tempfile::tempdir;

        // -------- build a table where EVERY column has a null‑mask ----------
        let mut cols = Vec::new();

        // numeric with mask
        let data_i32 = Vec64::from_slice(&(0..n as i32).collect::<Vec<_>>());
        let arr_i32 = IntegerArray::from_vec64(data_i32, Some(make_alternating_mask(n)));
        cols.push(FieldArray::new(
            Field::new("num32n", ArrowType::Int32, true, None),
            Array::NumericArray(NumericArray::Int32(Arc::new(arr_i32))),
        ));

        // bool with mask
        let bools = BooleanArray::from_slice(&(0..n).map(|i| i % 3 == 0).collect::<Vec<_>>());
        let mut bools_masked = bools.clone();
        bools_masked.null_mask = Some(make_alternating_mask(n));
        cols.push(FieldArray::new(
            Field::new("booln", ArrowType::Boolean, true, None),
            Array::BooleanArray(Arc::new(bools_masked)),
        ));

        // utf8 with mask
        let strings: Vec<String> = (0..n).map(|i| format!("s{i}")).collect();
        let refs: Vec<&str> = strings.iter().map(|s| &**s).collect();
        let str_arr = StringArray::from_vec(refs, Some(make_alternating_mask(n)));
        cols.push(FieldArray::new(
            Field::new("strn", ArrowType::String, true, None),
            Array::TextArray(TextArray::String32(Arc::new(str_arr))),
        ));

        // dictionary with mask
        let keys: Vec64<u32> = Vec64::from_slice(&(0..n as u32).map(|i| i % 2).collect::<Vec<_>>());
        let uniqs: Vec<String> = vec!["A".into(), "B".into()];
        let cat_arr = CategoricalArray::new(
            minarrow::Buffer::from(keys),
            Vec64::from(uniqs.clone()),
            Some(make_alternating_mask(n)),
        );
        cols.push(FieldArray::new(
            Field::new(
                "catn",
                ArrowType::Dictionary(CategoricalIndexType::UInt32),
                true,
                None,
            ),
            Array::TextArray(TextArray::Categorical32(Arc::new(cat_arr))),
        ));

        let table = Table {
            cols,
            n_rows: n,
            name: "nulls".into(),
        };

        // ---------- write to file -------------
        let dir = tempdir().unwrap();
        let path = dir.path().join("arrow_nulls_ipc.bin");
        let schema: Vec<Field> = table
            .cols
            .iter()
            .map(|c| c.field.as_ref().clone())
            .collect();
        {
            let file = tokio::fs::File::create(&path).await.unwrap();
            let mut wr = TableWriter::new(file, schema, fmt).unwrap();
            wr.register_dictionary(3, uniqs); // cat column is 3rd here
            wr.write_all_tables(vec![table.clone()]).await.unwrap();
        }

        // ---------- read using appropriate reader for protocol ----------
        let mut roundtripped = match fmt {
            IPCMessageProtocol::File => {
                let reader = FileTableReader::open(&path).unwrap();
                reader.read_batch(0).unwrap()
            }
            IPCMessageProtocol::Stream => {
                let stream = DiskByteStream::open(&path, BufferChunkSize::Custom(128 * 1024))
                    .await
                    .unwrap();
                let mut reader = TableStreamReader64::new(stream, 128 * 1024, IPCMessageProtocol::Stream);
                reader.next().await.unwrap().unwrap()
            }
        };
        
        // Fix table name to match original for comparison
        roundtripped.name = table.name.clone();
        assert_eq!(roundtripped, table); // derives PartialEq so includes masks
    }

    #[tokio::test]
    async fn test_file_roundtrip_via_disk() {
        use tempfile::tempdir;
        let n = 5;
        let table = build_all_types_table(n);
        let schema: Vec<Field> = table
            .cols
            .iter()
            .map(|c| c.field.as_ref().clone())
            .collect();

        // ---------- create a temp file ----------
        let dir = tempdir().unwrap();
        let p = dir.path().join("arrow.bin");
        {
            let file = tokio::fs::File::create(&p).await.unwrap();
            let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File).unwrap();
            for (id, u) in dicts_for_table(&table) {
                writer.register_dictionary(id, vec64_to_vec(u));
            }
            writer.write_all_tables(vec![table.clone()]).await.unwrap();
        }

        // ---------- read file via FileTableReader ----------
        let reader = FileTableReader::open(&p).unwrap();
        let mut roundtripped = reader.read_batch(0).unwrap();
        
        // Fix table name to match original for comparison
        roundtripped.name = table.name.clone();

        assert_eq!(roundtripped, table);
    }
}
