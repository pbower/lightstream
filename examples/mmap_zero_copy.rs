//! Memory-mapped zero-copy Arrow IPC example.
//!
//! This example demonstrates how to:
//! - Create a table with sample data using Vec64 for alignment  
//! - Write it to Arrow IPC File format using TableWriter
//! - Read it back using MmapTableReader for zero-copy access
//! - Show the performance benefits of memory mapping vs regular file I/O

use lightstream_io::enums::IPCMessageProtocol;
use lightstream_io::models::writers::ipc::table_writer::TableWriter;
use lightstream_io::models::readers::ipc::file_table_reader::FileTableReader;
#[cfg(feature = "mmap")]
use lightstream_io::models::readers::ipc::mmap_table_reader::MmapTableReader;
use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{Array, Field, FieldArray, NumericArray, Table, TextArray, Vec64, Buffer, IntegerArray, FloatArray, StringArray, BooleanArray, Bitmask};
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::fs::File;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Memory-Mapped Zero-Copy Example");
    println!("==============================");

    // Create large sample data to show performance benefits
    let table = create_large_table();
    println!("Created table '{}' with {} rows and {} columns", 
             table.name, table.n_rows, table.cols.len());

    // Create a temporary directory for our example
    let temp_dir = tempdir()?;
    let file_path = temp_dir.path().join("large_sample.arrow");

    // Write to Arrow IPC File format
    println!("\n1. Writing to Arrow IPC File");
    write_arrow_file(&table, &file_path).await?;

    // Read using regular file I/O
    println!("\n2. Reading with Regular File I/O");
    read_with_file_io(&file_path)?;

    // Read using memory mapping (zero-copy)
    #[cfg(feature = "mmap")]
    {
        println!("\n3. Reading with Memory Mapping (Zero-Copy)");
        read_with_mmap(&file_path)?;
    }

    #[cfg(not(feature = "mmap"))]
    println!("\n3. Memory mapping not available (mmap feature not enabled)");

    println!("\n✓ Memory-mapped zero-copy example completed!");

    Ok(())
}

/// Create a large table with Vec64-aligned data for zero-copy benefits
fn create_large_table() -> Table {
    let n_rows = 10_000; // Reduced size to prevent freezing
    
    // Create integer column with Vec64 for 64-byte alignment (collect directly into Vec64)
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64).collect();
    let int_array = Array::NumericArray(NumericArray::Int64(Arc::new(IntegerArray {
        data: Buffer::from(int_data),
        null_mask: None,
    })));
    let int_field = FieldArray::new(
        Field {
            name: "id".into(),
            dtype: ArrowType::Int64,
            nullable: false,
            metadata: Default::default(),
        },
        int_array,
    );

    // Create float column with Vec64 for alignment (collect directly into Vec64)
    let float_data: Vec64<f64> = (0..n_rows).map(|i| (i as f64) * 0.001 + 3.14159).collect();
    let float_array = Array::NumericArray(NumericArray::Float64(Arc::new(FloatArray {
        data: Buffer::from(float_data),
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

    // Create string column with simpler, shorter strings (using Vec64 from the start)
    let base_str = "record_"; // Fixed prefix
    let mut str_data = Vec64::new();
    let mut offsets = Vec64::with_capacity(n_rows + 1);
    offsets.push(0u32);
    
    for i in 0..n_rows {
        let record_str = format!("{}{:04}", base_str, i % 1000); // Cycle every 1000 for efficiency
        str_data.extend_from_slice(record_str.as_bytes());
        offsets.push(str_data.len() as u32);
    }
    
    let str_array = Array::TextArray(TextArray::String32(Arc::new(StringArray::new(
        Buffer::from(str_data),
        None,
        Buffer::from(offsets),
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

    // Create boolean column with Vec64-aligned bitmask (using Vec64 directly)
    let byte_count = (n_rows + 7) / 8;
    let mut bitmask_bytes = Vec64::with_capacity(byte_count);
    bitmask_bytes.resize(byte_count, 0u8);
    for i in 0..n_rows {
        if i % 3 == 0 {
            bitmask_bytes[i / 8] |= 1 << (i % 8);
        }
    }
    let bool_array = Array::BooleanArray(Arc::new(BooleanArray {
        data: Bitmask::from_bytes(&bitmask_bytes, n_rows),
        null_mask: None,
        len: n_rows,
        _phantom: std::marker::PhantomData,
    }));
    let bool_field = FieldArray::new(
        Field {
            name: "is_divisible_by_3".into(),
            dtype: ArrowType::Boolean,
            nullable: false,
            metadata: Default::default(),
        },
        bool_array,
    );

    Table {
        name: "large_aligned_data".to_string(),
        n_rows,
        cols: vec![int_field, float_field, str_field, bool_field],
    }
}

/// Write table to Arrow IPC File format
async fn write_arrow_file(table: &Table, file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    {
        let file = File::create(file_path).await?;
        let schema: Vec<Field> = table.cols.iter().map(|col| (*col.field).clone()).collect();
        let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;
        writer.write_all_tables(vec![table.clone()]).await?;
    }
    let write_time = start.elapsed();
    
    let file_size = tokio::fs::metadata(file_path).await?.len();
    println!("  Wrote {} bytes in {:?}", file_size, write_time);
    println!("  File size: {:.2} MB", file_size as f64 / (1024.0 * 1024.0));
    
    Ok(())
}

/// Read using regular file I/O (copies data to heap)
fn read_with_file_io(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    
    let reader = FileTableReader::open(file_path)?;
    let table = reader.read_batch(0)?;
    
    let read_time = start.elapsed();
    
    println!("  Regular I/O read: {:?}", read_time);
    println!("  Read {} rows, {} columns", table.n_rows, table.cols.len());
    
    // Show some sample data
    if let Array::NumericArray(NumericArray::Int64(int_arr)) = &table.cols[0].array {
        println!("  Sample int data: {:?}", &int_arr.data.as_ref()[0..5]);
    }
    
    Ok(())
}

/// Read using memory mapping (zero-copy access)
#[cfg(feature = "mmap")]
fn read_with_mmap(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    
    let reader = MmapTableReader::open(file_path)?;
    let table = reader.read_batch(0)?;
    
    let read_time = start.elapsed();
    
    println!("  Memory-mapped read: {:?}", read_time);
    println!("  Read {} rows, {} columns (zero-copy)", table.n_rows, table.cols.len());
    
    // Show some sample data - this is accessing memory-mapped data directly!
    if let Array::NumericArray(NumericArray::Int64(int_arr)) = &table.cols[0].array {
        println!("  Sample int data (mmap): {:?}", &int_arr.data.as_ref()[0..5]);
    }
    
    // Demonstrate the zero-copy benefit
    println!("  ✓ Data accessed directly from memory-mapped file (no copying!)");
    
    Ok(())
}