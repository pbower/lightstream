//! TableStreamReader example for reading streaming Arrow IPC data.
//!
//! This example demonstrates how to:
//! - Create streaming Arrow IPC data using TableStreamWriter
//! - Read it back using TableStreamReader for chunk-by-chunk processing
//! - Handle different stream protocols (Stream vs File)
//! - Process large datasets without loading everything into memory

use lightstream_io::enums::IPCMessageProtocol;
use lightstream_io::models::writers::ipc::table_stream_writer::TableStreamWriter;
use lightstream_io::models::readers::ipc::table_stream_reader::TableStreamReader64;
use lightstream_io::models::streams::disk::DiskByteStream;
use lightstream_io::enums::BufferChunkSize;
use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{Array, Field, FieldArray, NumericArray, Table, TextArray, Vec64, Buffer, IntegerArray, FloatArray, StringArray, BooleanArray, Bitmask};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("TableStreamReader Example");
    println!("========================");

    // Create a temporary directory for our examples
    let temp_dir = tempdir()?;

    // Example 1: Stream protocol reading
    println!("\n1. Stream Protocol Reading");
    let stream_path = temp_dir.path().join("data.stream");
    stream_protocol_example(&stream_path).await?;

    // Example 2: File protocol reading (for comparison)
    println!("\n2. File Protocol Reading");
    let file_path = temp_dir.path().join("data.arrow");
    file_protocol_example(&file_path).await?;

    // Example 3: Large dataset chunk processing
    println!("\n3. Large Dataset Chunk Processing");
    let large_path = temp_dir.path().join("large_data.stream");
    large_dataset_example(&large_path).await?;

    println!("\n✓ TableStreamReader example completed successfully!");

    Ok(())
}

/// Demonstrate Stream protocol reading
async fn stream_protocol_example(stream_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // First, create sample data in Stream format
    let tables = create_small_sample_tables(3);
    write_stream_data(&tables, stream_path, IPCMessageProtocol::Stream).await?;

    println!("  Reading Stream protocol data...");
    
    // Read using TableStreamReader
    let disk_stream = DiskByteStream::open(stream_path, BufferChunkSize::Custom(8192)).await?;
    let mut reader = TableStreamReader64::new(disk_stream, 8192, IPCMessageProtocol::Stream);
    
    let mut batch_count = 0;
    let mut total_rows = 0;
    
    while let Some(result) = reader.next().await {
        let table = result?;
        batch_count += 1;
        total_rows += table.n_rows;
        
        println!("    Batch {}: {} rows", batch_count, table.n_rows);
        
        // Show sample data
        if let Array::NumericArray(NumericArray::Int32(int_arr)) = &table.cols[0].array {
            if table.n_rows > 0 {
                println!("      First value: {}", int_arr.data.as_ref()[0]);
            }
        }
    }
    
    println!("  ✓ Read {} batches, {} total rows", batch_count, total_rows);
    Ok(())
}

/// Demonstrate File protocol reading
async fn file_protocol_example(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create sample data in File format
    let tables = create_small_sample_tables(2);
    write_stream_data(&tables, file_path, IPCMessageProtocol::File).await?;

    println!("  Reading File protocol data...");
    
    // Read using TableStreamReader with File protocol
    let disk_stream = DiskByteStream::open(file_path, BufferChunkSize::Custom(8192)).await?;
    let mut reader = TableStreamReader64::new(disk_stream, 8192, IPCMessageProtocol::File);
    
    let mut batch_count = 0;
    let mut total_rows = 0;
    
    while let Some(result) = reader.next().await {
        let table = result?;
        batch_count += 1;
        total_rows += table.n_rows;
        
        println!("    Batch {}: {} rows", batch_count, table.n_rows);
    }
    
    println!("  ✓ Read {} batches, {} total rows", batch_count, total_rows);
    Ok(())
}

/// Demonstrate processing large datasets chunk by chunk
async fn large_dataset_example(large_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    // Create larger dataset
    let large_tables = create_large_sample_tables(10);
    let total_expected_rows: usize = large_tables.iter().map(|t| t.n_rows).sum();
    
    write_stream_data(&large_tables, large_path, IPCMessageProtocol::Stream).await?;
    
    println!("  Processing large dataset ({} total expected rows)...", total_expected_rows);
    
    // Use smaller chunk size to demonstrate streaming
    let disk_stream = DiskByteStream::open(large_path, BufferChunkSize::Custom(4096)).await?;
    let mut reader = TableStreamReader64::new(disk_stream, 4096, IPCMessageProtocol::Stream);
    
    let mut batch_count = 0;
    let mut total_rows = 0;
    let mut sum_of_values = 0.0;
    
    let start_time = std::time::Instant::now();
    
    // Process each batch as it arrives (streaming fashion)
    while let Some(result) = reader.next().await {
        let table = result?;
        batch_count += 1;
        total_rows += table.n_rows;
        
        // Perform some computation on each batch without storing it
        if let Array::NumericArray(NumericArray::Float64(float_arr)) = &table.cols[1].array {
            for &value in float_arr.data.as_ref() {
                sum_of_values += value;
            }
        }
        
        // Progress indicator
        if batch_count % 5 == 0 {
            println!("    Processed {} batches so far...", batch_count);
        }
    }
    
    let duration = start_time.elapsed();
    
    println!("  ✓ Processed {} batches, {} total rows in {:?}", batch_count, total_rows, duration);
    println!("    Average value: {:.2}", sum_of_values / total_rows as f64);
    println!("    Memory-efficient: each batch processed and discarded");
    
    Ok(())
}

/// Create small sample tables for testing
fn create_small_sample_tables(num_tables: usize) -> Vec<Table> {
    let mut tables = Vec::new();
    
    for i in 0..num_tables {
        let n_rows = 100;
        let start_val = i * 100;
        
        // Integer column
        let int_data: Vec<i32> = (start_val..start_val + n_rows).map(|x| x as i32).collect();
        let int_array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&int_data)),
            null_mask: None,
        })));
        let int_field = FieldArray::new(
            Field {
                name: "sequence_id".into(),
                dtype: ArrowType::Int32,
                nullable: false,
                metadata: Default::default(),
            },
            int_array,
        );

        // String column
        let individual_strings: Vec<String> = (0..n_rows)
            .map(|j| format!("table_{}_row_{:03}", i, j))
            .collect();
        let mut str_data = Vec::new();
        let mut offsets = Vec::with_capacity(n_rows + 1);
        offsets.push(0u32);
        for s in &individual_strings {
            str_data.extend_from_slice(s.as_bytes());
            offsets.push(str_data.len() as u32);
        }
        let str_array = Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
            Buffer::from(Vec64::from_slice(&str_data)),
            None,
            Buffer::from(Vec64::from_slice(&offsets)),
        ))));
        let str_field = FieldArray::new(
            Field {
                name: "label".into(),
                dtype: ArrowType::String,
                nullable: false,
                metadata: Default::default(),
            },
            str_array,
        );

        tables.push(Table {
            name: format!("small_table_{}", i),
            n_rows,
            cols: vec![int_field, str_field],
        });
    }
    
    tables
}

/// Create larger sample tables for performance testing
fn create_large_sample_tables(num_tables: usize) -> Vec<Table> {
    let mut tables = Vec::new();
    
    for i in 0..num_tables {
        let n_rows = 5000; // Larger batches
        let start_val = i * 5000;
        
        // Integer column
        let int_data: Vec<i32> = (start_val..start_val + n_rows).map(|x| x as i32).collect();
        let int_array = Array::NumericArray(NumericArray::Int32(Arc::new(IntegerArray {
            data: Buffer::from(Vec64::from_slice(&int_data)),
            null_mask: None,
        })));
        let int_field = FieldArray::new(
            Field {
                name: "id".into(),
                dtype: ArrowType::Int32,
                nullable: false,
                metadata: Default::default(),
            },
            int_array,
        );

        // Float column with some computation
        let float_data: Vec<f64> = (0..n_rows)
            .map(|j| (j as f64 + i as f64 * 1000.0) * 0.01 + 42.0)
            .collect();
        let float_array = Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
            data: Buffer::from(Vec64::from_slice(&float_data)),
            null_mask: None,
        })));
        let float_field = FieldArray::new(
            Field {
                name: "measurement".into(),
                dtype: ArrowType::Float64,
                nullable: false,
                metadata: Default::default(),
            },
            float_array,
        );

        // Boolean column
        let bool_data: Vec<bool> = (0..n_rows).map(|j| (i + j) % 7 == 0).collect();
        let bitmask_bytes = {
            let mut bytes = vec![0u8; (n_rows + 7) / 8];
            for (j, &value) in bool_data.iter().enumerate() {
                if value {
                    bytes[j / 8] |= 1 << (j % 8);
                }
            }
            bytes
        };
        let bool_array = Array::BooleanArray(Arc::new(BooleanArray {
            data: Bitmask::from_bytes(&bitmask_bytes, n_rows),
            null_mask: None,
            len: n_rows,
            _phantom: std::marker::PhantomData,
        }));
        let bool_field = FieldArray::new(
            Field {
                name: "is_special".into(),
                dtype: ArrowType::Boolean,
                nullable: false,
                metadata: Default::default(),
            },
            bool_array,
        );

        tables.push(Table {
            name: format!("large_batch_{}", i),
            n_rows,
            cols: vec![int_field, float_field, bool_field],
        });
    }
    
    tables
}

/// Write tables to stream using TableStreamWriter
async fn write_stream_data(
    tables: &[Table],
    output_path: &Path,
    protocol: IPCMessageProtocol,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema: Vec<Field> = tables[0].cols.iter().map(|col| (*col.field).clone()).collect();
    let mut stream_writer = TableStreamWriter::<Vec64<u8>>::new(schema, protocol);
    
    // Write all tables
    for table in tables {
        stream_writer.write(table)?;
    }
    stream_writer.finish()?;
    
    // Write to file
    let mut file = File::create(output_path).await?;
    while let Some(frame_result) = stream_writer.next_frame() {
        let frame = frame_result?;
        file.write_all(frame.as_ref()).await?;
    }
    file.flush().await?;
    
    Ok(())
}