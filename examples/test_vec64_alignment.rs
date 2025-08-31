//! Test Vec64 SharedBuffer alignment without file I/O
//! 
//! This tests whether minarrow's SharedBuffer system works correctly
//! with Vec64 alignment, without involving any file I/O that might
//! disrupt alignment.

use minarrow::ffi::arrow_dtype::ArrowType;
use minarrow::{Array, Field, FieldArray, NumericArray, Table, TextArray, Vec64, Buffer, IntegerArray, StringArray};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Vec64 SharedBuffer Alignment (No File I/O)");
    println!("================================================");

    // Test 1: Create Vec64 data and verify alignment
    println!("\n1. Testing Vec64 Creation and Alignment");
    test_vec64_creation();

    // Test 2: Create Table with Vec64 and test SharedBuffer behavior
    println!("\n2. Testing Table with Vec64 Arrays");
    test_table_with_vec64();

    // Test 3: Test Buffer::from_shared behavior directly
    println!("\n3. Testing Buffer::from_shared with Vec64");
    test_buffer_from_shared();

    println!("\n✓ Vec64 alignment tests completed!");
    Ok(())
}

fn test_vec64_creation() {
    println!("  Creating Vec64 with 10K integers...");
    
    // Create Vec64 directly
    let int_data: Vec64<i64> = (0..10000).map(|i| i as i64).collect();
    
    println!("  Vec64 length: {}", int_data.len());
    println!("  Vec64 capacity: {}", int_data.capacity());
    
    // Check alignment of the underlying pointer
    let ptr = int_data.as_ptr() as usize;
    println!("  Vec64 pointer: 0x{:x}", ptr);
    println!("  64-byte aligned: {}", ptr % 64 == 0);
    
    // Create Buffer from Vec64
    let buffer = Buffer::from(int_data);
    println!("  Buffer created successfully");
    
    // Check if it's shared
    println!("  Buffer is_shared: {}", buffer.is_shared());
}

fn test_table_with_vec64() {
    println!("  Creating Table with Vec64 arrays...");
    
    let n_rows = 1000;
    
    // Integer column with Vec64
    let int_data: Vec64<i64> = (0..n_rows).map(|i| i as i64).collect();
    println!("  Integer Vec64 pointer: 0x{:x} (aligned: {})", 
             int_data.as_ptr() as usize, 
             int_data.as_ptr() as usize % 64 == 0);
    
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

    // String column with Vec64
    let mut str_data = Vec64::new();
    let mut offsets = Vec64::with_capacity(n_rows + 1);
    offsets.push(0u32);
    
    for i in 0..n_rows {
        let s = format!("item_{:04}", i);
        str_data.extend_from_slice(s.as_bytes());
        offsets.push(str_data.len() as u32);
    }
    
    println!("  String data Vec64 pointer: 0x{:x} (aligned: {})", 
             str_data.as_ptr() as usize, 
             str_data.as_ptr() as usize % 64 == 0);
    println!("  String offsets Vec64 pointer: 0x{:x} (aligned: {})", 
             offsets.as_ptr() as usize, 
             offsets.as_ptr() as usize % 64 == 0);

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

    let table = Table {
        name: "test_vec64".to_string(),
        n_rows,
        cols: vec![int_field, str_field],
    };

    println!("  Table created with {} rows, {} columns", table.n_rows, table.cols.len());
    
    // Check if the arrays in the table maintain their shared status
    for (i, col) in table.cols.iter().enumerate() {
        match &col.array {
            Array::NumericArray(NumericArray::Int64(arr)) => {
                println!("  Column {}: Integer array is_shared: {}", i, arr.data.is_shared());
            }
            Array::TextArray(TextArray::String32(arr)) => {
                println!("  Column {}: String data is_shared: {}", i, arr.data.is_shared());
                println!("  Column {}: String offsets is_shared: {}", i, arr.offsets.is_shared());
            }
            _ => {}
        }
    }
}

fn test_buffer_from_shared() {
    println!("  Testing Buffer::from_shared behavior...");
    
    // Create aligned Vec64
    let vec64_data: Vec64<i64> = (0..1000).map(|i| i as i64).collect();
    let ptr = vec64_data.as_ptr() as usize;
    println!("  Original Vec64 pointer: 0x{:x} (aligned: {})", ptr, ptr % 64 == 0);
    
    // Convert to bytes for SharedBuffer test
    let byte_slice = unsafe {
        std::slice::from_raw_parts(
            vec64_data.as_ptr() as *const u8,
            vec64_data.len() * std::mem::size_of::<i64>()
        )
    };
    
    // Test what happens when we create a SharedBuffer from this
    // This simulates what happens when reading from mmap
    println!("  Byte slice pointer: 0x{:x} (aligned: {})", 
             byte_slice.as_ptr() as usize, 
             byte_slice.as_ptr() as usize % 64 == 0);
    
    // Create a Buffer from the byte slice (this is similar to what mmap would do)
    let buffer = Buffer::from_slice(byte_slice);
    println!("  Buffer from slice is_shared: {}", buffer.is_shared());
    
    // Now test creating an IntegerArray from this buffer
    let int_array = IntegerArray {
        data: buffer,
        null_mask: None,
    };
    
    println!("  IntegerArray data is_shared: {}", int_array.data.is_shared());
    
    // Keep the original data alive
    std::mem::forget(vec64_data);
}