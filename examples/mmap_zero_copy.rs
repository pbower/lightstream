//! Memory-mapped zero-copy Arrow IPC example.
//!
//! This example demonstrates how to:
//! - Create a table with sample data using Vec64 for alignment  
//! - Write it to Arrow IPC File format using TableWriter
//! - Read it back using MmapTableReader for zero-copy access
//! - Show the performance benefits of memory mapping vs regular file I/O

use lightstream_io::enums::IPCMessageProtocol;
use lightstream_io::models::writers::ipc::table_writer::TableWriter;
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

    // Read using memory mapping (zero-copy)
    #[cfg(feature = "mmap")]
    {
        println!("\n2. Reading with Memory Mapping (Zero-Copy)");
        read_with_mmap(&file_path)?;
    }

    #[cfg(not(feature = "mmap"))]
    println!("\n2. Memory mapping not available (mmap feature not enabled)");

    println!("\n✓ Memory-mapped zero-copy example completed!");

    Ok(())
}

/// Create a large table with pre-built standard buffers
fn create_large_table() -> Table {
    let n_rows = 100_000;
    
    // Pre-create standard string data - just repeat "test_string" pattern
    let test_string = b"test_string";
    let string_len = test_string.len();
    let total_string_bytes = n_rows * string_len;
    
    let mut str_data = Vec64::with_capacity(total_string_bytes);
    let mut offsets = Vec64::with_capacity(n_rows + 1);
    offsets.push(0u32);
    
    for _ in 0..n_rows {
        str_data.extend_from_slice(test_string);
        offsets.push(str_data.len() as u32);
    }
    
    // Create arrays from pre-built buffers
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64).collect();
    let float_data: Vec64<f64> = (0..n_rows).map(|i| i as f64 * 0.1).collect();
    
    let mut bitmask = Bitmask::with_capacity(n_rows);
    for i in 0..n_rows {
        bitmask.set(i, i % 2 == 0);
    }
    
    // Build table with pre-created buffers
    let int_array = Arc::new(IntegerArray::new(Buffer::from(int_data), None));
    let float_array = Arc::new(FloatArray::new(Buffer::from(float_data), None));
    let str_array = Arc::new(StringArray::new(Buffer::from(str_data), None, Buffer::from(offsets)));
    let bool_array = Arc::new(BooleanArray::new(bitmask, None));
    
    Table {
        name: "large_aligned_data".to_string(),
        n_rows,
        cols: vec![
            FieldArray::new(
                Field { name: "id".into(), dtype: ArrowType::Int64, nullable: false, metadata: Default::default() },
                int_array.into()
            ),
            FieldArray::new(
                Field { name: "measurement".into(), dtype: ArrowType::Float64, nullable: false, metadata: Default::default() },
                Array::NumericArray(NumericArray::Float64(float_array))
            ),
            FieldArray::new(
                Field { name: "label".into(), dtype: ArrowType::String, nullable: false, metadata: Default::default() },
                Array::TextArray(TextArray::String32(str_array))
            ),
            FieldArray::new(
                Field { name: "is_even".into(), dtype: ArrowType::Boolean, nullable: false, metadata: Default::default() },
                Array::BooleanArray(bool_array)
            ),
        ],
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


/// Read using memory mapping
#[cfg(feature = "mmap")]
fn read_with_mmap(file_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    
    let reader = MmapTableReader::open(file_path)?;
    let table = reader.read_batch(0)?;
    
    let read_time = start.elapsed();
    
    println!("  Memory-mapped read: {:?}", read_time);
    println!("  Read {} rows, {} columns (zero-copy)", table.n_rows, table.cols.len());
    
    // Show some sample data - access memory-mapped data directly
    if let Array::NumericArray(NumericArray::Int64(int_arr)) = &table.cols[0].array {
        println!("  Sample int data (mmap): {:?}", &int_arr.data.as_ref()[0..5]);
    }
       
    println!("  ✓ Data accessed directly from memory-mapped file (no copying!)");
    
    Ok(())
}