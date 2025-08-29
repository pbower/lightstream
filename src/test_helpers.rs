use std::sync::Arc;

use minarrow::{
    Array, ArrowType, Bitmask, Buffer, CategoricalArray, FieldArray, Field, FloatArray,
    IntegerArray, NumericArray, Table, TextArray, Vec64
};
#[cfg(feature = "datetime")]
use minarrow::{DatetimeArray, TemporalArray};
use tempfile::NamedTempFile;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

use crate::{enums::IPCMessageProtocol, models::writers::ipc::{table_stream_writer::write_tables_to_stream, table_writer::write_tables_to_file}};

    // -------------------- Column Generators -------------------- //

    pub (crate) fn int32_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "int32".into(),
                dtype: ArrowType::Int32,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[1, 2, 3, 4])),
                null_mask: None
            })))
        )
    }

    pub (crate) fn int64_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "int64".into(),
                dtype: ArrowType::Int64,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[11, 12, 13, 14])),
                null_mask: None
            })))
        )
    }

    pub (crate) fn uint32_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "uint32".into(),
                dtype: ArrowType::UInt32,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::UInt32(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[21u32, 22, 23, 24])),
                null_mask: None
            })))
        )
    }

    pub (crate) fn uint64_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "uint64".into(),
                dtype: ArrowType::UInt64,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::UInt64(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[31u64, 32, 33, 34])),
                null_mask: None
            })))
        )
    }

    pub (crate) fn float32_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "float32".into(),
                dtype: ArrowType::Float32,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::Float32(Arc::new(FloatArray {
                data: Buffer::from(Vec64::from_slice(&[0.1f32, 0.2, 0.3, 0.4])),
                null_mask: None
            })))
        )
    }

    pub (crate) fn float64_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "float64".into(),
                dtype: ArrowType::Float64,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
                data: Buffer::from(Vec64::from_slice(&[1.1f64, 2.2, 3.3, 4.4])),
                null_mask: None
            })))
        )
    }

    pub (crate) fn bool_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "bool".into(),
                dtype: ArrowType::Boolean,
                nullable: true,
                metadata: Default::default()
            },
            Array::BooleanArray(Arc::new(minarrow::BooleanArray {
                data: Bitmask::from_bytes(&[0b00001101], 4),
                null_mask: Some(Bitmask::new_set_all(4, true)),
                len: 4,
                _phantom: std::marker::PhantomData
            }))
        )
    }

    pub (crate) fn string32_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "string32".into(),
                dtype: ArrowType::String,
                nullable: true,
                metadata: Default::default()
            },
            Array::TextArray(TextArray::String32(Arc::new(minarrow::StringArray::new(
                Buffer::from(Vec64::from_slice("helloworldxx".as_bytes())),
                Some(Bitmask::new_set_all(4, true)),
                Buffer::from(Vec64::from_slice(&[0u32, 4, 9, 11, 12]))
            ))))
        )
    }

    pub (crate) fn dict32_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "dict32".into(),
                dtype: ArrowType::Dictionary(
                    minarrow::ffi::arrow_dtype::CategoricalIndexType::UInt32
                ),
                nullable: true,
                metadata: Default::default()
            },
            Array::TextArray(TextArray::Categorical32(Arc::new(CategoricalArray {
                data: Buffer::from(Vec64::from_slice(&[1u32, 0, 2, 1])),
                unique_values: Vec64::from(vec![
                    "apple".to_string(),
                    "banana".to_string(),
                    "pear".to_string(),
                ]),
                null_mask: Some(Bitmask::new_set_all(4, true))
            })))
        )
    }

    #[cfg(feature = "datetime")]
    pub (crate) fn dt32_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "dt32".into(),
                dtype: ArrowType::Date32,
                nullable: false,
                metadata: Default::default()
            },
            Array::TemporalArray(TemporalArray::Datetime32(Arc::new(minarrow::DatetimeArray {
                data: Buffer::from(Vec64::from_slice(&[111, 222, 333, 444])),
                null_mask: None,
                time_unit: minarrow::TimeUnit::Days,
            })))
        )
    }

    #[cfg(feature = "datetime")]
    pub (crate) fn dt64_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "dt64".into(),
                dtype: ArrowType::Date64,
                nullable: false,
                metadata: Default::default()
            },
            Array::TemporalArray(TemporalArray::Datetime64(Arc::new(minarrow::DatetimeArray {
                data: Buffer::from(Vec64::from_slice(&[1111111, 2222222, 3333333, 4444444])),
                null_mask: None,
                time_unit: minarrow::TimeUnit::Days,
            })))
        )
    }

    #[cfg(feature = "large_string")]
    pub (crate) fn string64_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "string64".into(),
                dtype: ArrowType::LargeString,
                nullable: true,
                metadata: Default::default()
            },
            Array::TextArray(TextArray::String64(Arc::new(minarrow::StringArray::new(
                Buffer::from(Vec64::from_slice("bigbigstringlarge".as_bytes())),
                Some(Bitmask::new_set_all(4, true)),
                Buffer::from(Vec64::from_slice(&[0u64, 5, 11, 16, 17]))
            ))))
        )
    }

    #[cfg(feature = "extended_categorical")]
    pub (crate) fn dict8_col() -> FieldArray {
        use minarrow::ffi::arrow_dtype::CategoricalIndexType::*;
        FieldArray::new(
            Field {
                name: "dict8".into(),
                dtype: ArrowType::Dictionary(UInt8),
                nullable: true,
                metadata: Default::default()
            },
            Array::TextArray(TextArray::Categorical8(Arc::new(CategoricalArray {
                data: Buffer::from(Vec64::from_slice(&[1u8, 0, 2, 1])),
                unique_values: Vec64::from(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                null_mask: Some(Bitmask::new_set_all(4, true))
            })))
        )
    }

    #[cfg(feature = "extended_categorical")]
    pub (crate) fn dict16_col() -> FieldArray {
        use minarrow::ffi::arrow_dtype::CategoricalIndexType::*;
        FieldArray::new(
            Field {
                name: "dict16".into(),
                dtype: ArrowType::Dictionary(UInt16),
                nullable: true,
                metadata: Default::default()
            },
            Array::TextArray(TextArray::Categorical16(Arc::new(CategoricalArray {
                data: Buffer::from(Vec64::from_slice(&[1u16, 0, 2, 1])),
                unique_values: Vec64::from(vec!["x".to_string(), "y".to_string(), "z".to_string()]),
                null_mask: Some(Bitmask::new_set_all(4, true))
            })))
        )
    }

    #[cfg(feature = "extended_categorical")]
    pub (crate) fn dict64_col() -> FieldArray {
        use minarrow::ffi::arrow_dtype::CategoricalIndexType::*;
        FieldArray::new(
            Field {
                name: "dict64".into(),
                dtype: ArrowType::Dictionary(UInt64),
                nullable: true,
                metadata: Default::default()
            },
            Array::TextArray(TextArray::Categorical64(Arc::new(CategoricalArray {
                data: Buffer::from(Vec64::from_slice(&[1u64, 0, 2, 1])),
                unique_values: Vec64::from(vec![
                    "long".to_string(),
                    "lo".to_string(),
                    "l".to_string(),
                ]),
                null_mask: Some(Bitmask::new_set_all(4, true))
            })))
        )
    }

    #[cfg(feature = "extended_numeric_types")]
    pub (crate) fn int8_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "int8".into(),
                dtype: ArrowType::Int8,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::Int8(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[5i8, 6, 7, 8])),
                null_mask: None
            })))
        )
    }

    #[cfg(feature = "extended_numeric_types")]
    pub (crate) fn int16_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "int16".into(),
                dtype: ArrowType::Int16,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::Int16(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[15i16, 16, 17, 18])),
                null_mask: None
            })))
        )
    }

    #[cfg(feature = "extended_numeric_types")]
    pub (crate) fn uint8_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "uint8".into(),
                dtype: ArrowType::UInt8,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::UInt8(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[25u8, 26, 27, 28])),
                null_mask: None
            })))
        )
    }

    #[cfg(feature = "extended_numeric_types")]
    pub (crate) fn uint16_col() -> FieldArray {
        FieldArray::new(
            Field {
                name: "uint16".into(),
                dtype: ArrowType::UInt16,
                nullable: false,
                metadata: Default::default()
            },
            Array::NumericArray(NumericArray::UInt16(Arc::new(IntegerArray {
                data: Buffer::from(Vec64::from_slice(&[35u16, 36, 37, 38])),
                null_mask: None
            })))
        )
    }

    // -------------------- Table Generator -------------------- //

    pub (crate) fn make_all_types_table() -> Table {
        let mut cols = vec![
            int32_col(),
            int64_col(),
            uint32_col(),
            uint64_col(),
            float32_col(),
            float64_col(),
            bool_col(),
            string32_col(),
            dict32_col(),
        ];
        #[cfg(feature = "datetime")]
        {
            cols.push(dt32_col());
            cols.push(dt64_col());
        }
        #[cfg(feature = "large_string")]
        cols.push(string64_col());
        // #[cfg(feature = "extended_categorical")]
        // {
        //     cols.push(dict8_col());
        //     cols.push(dict16_col());
        //     cols.push(dict64_col());
        // }
        #[cfg(feature = "extended_numeric_types")]
        {
            cols.push(int8_col());
            cols.push(int16_col());
            cols.push(uint8_col());
            cols.push(uint16_col());
        }
        Table {
            cols,
            n_rows: 4,
            name: "all_types".to_owned()
        }
    }

    pub (crate) fn make_schema_all_types() -> Vec<Field> {
        make_all_types_table().cols.iter().map(|fa| fa.field.as_ref().clone()).collect()
    }

    // -------------------- File Writers -------------------- //

    pub (crate) async fn write_test_table_to_file(tables: &[Table]) -> NamedTempFile {
        let temp = NamedTempFile::new().expect("Failed to create NamedTempFile");
        let path = temp.path().to_str().unwrap();
        let schema = make_schema_all_types();
    
        write_tables_to_file(path, tables, schema)
            .await
            .expect("Failed to write tables");
        temp
    }

    


/// Helper: Write tables to an in-memory async stream, then collect and chunk the result.
/// - tables: borrowed slice, not moved or cloned
/// - schema: borrowed slice
/// - protocol: file/stream
/// - chunk_size: number of bytes per simulated stream chunk
pub async fn write_test_table_to_ipc_stream(
    tables: &[Table],
    schema: &[Field],
    protocol: IPCMessageProtocol,
    chunk_size: usize,
) -> std::io::Result<Vec<Vec<u8>>> {
    let (mut tx, mut rx) = duplex(16 * 1024);

    write_tables_to_stream::<_, Vec64<u8>>(
        &mut tx,
        tables,
        schema.to_vec(),
        protocol,
    ).await?;
    tx.shutdown().await.ok();

    // Read all bytes from the reader end
    let mut all_bytes = Vec::new();
    let mut buf = vec![0u8; 8192];
    loop {
        let n = rx.read(&mut buf).await?;
        if n == 0 { break; }
        all_bytes.extend_from_slice(&buf[..n]);
    }

    // Split into Vec<Vec<u8>> chunks for streaming
    let mut chunks = Vec::new();
    let mut idx = 0;
    while idx < all_bytes.len() {
        let end = usize::min(idx + chunk_size, all_bytes.len());
        chunks.push(all_bytes[idx..end].to_vec());
        idx = end;
    }
    Ok(chunks)
}
