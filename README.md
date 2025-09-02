# Lightstream – *Drive your data live*

Lightstream is an extension to the Minarrow crate for adding native streaming capabilities and IO. It lets one build up raw byte streams, and then read and write the IPC and other wire formats such as TLV. It also includes standard CSV and Parquet writers, and an ultra-fast 64-byte SIMD memory-map IPC reader.

## Intro

**Lightstream** gives you access to composable building blocks for high-performance data I/O in Rust.
It extends [Minarrow](https://crates.io/crates/minarrow) with a set of modular, format-aware components for:
- High-performance asynchronous Arrow IPC streaming and file writing
- Framed decoders and sinks for `IPC`, `TLV`, `CSV`, and opt-in `Parquet`
- Zero-Copy memory-mapped Arrow file reads **(~4.5ms reads for 100m rows x 4 columns on a consumer laptop)**
- Direct Tokio integration with zero-copy buffers
- 64-byte SIMD aligned readers and writers *(the only Arrow-compatible crate that provides this in 2025)*

## Design Principles
- **Customisable** - ***You own the buffer*** – Plug your buffer. All streaming is pull-based or sink-driven.
- **Composable** - ***Layerable codecs*** – Each encoder, decoder, sink, and stream adapter is layerable, and your bytestream propagates up.
- **Control** - ***Wire-level framing*** – Arrow IPC, TLV, CSV, and Parquet handled at the transport boundary, not fused into business logic.
- **Compatible** - native streaming on futures, and Tokio.
- **Power** - ***64-byte aligned by default*** – All buffers use 64-byte aligned memory via [`Vec64`] supporting deterministic SIMD - not re-allocating during hotloop calculations where you need it fast.
- **Extensible** - all primitive batteries are included to create your own data wire formats, and customise it to your stack. We also welcome format contributions, as all the base structures and patterns are in-place.
- **Fast compile times** minimal dependencies.

## Layered Abstractions

| Layer                    | Provided by Lightstream        | Replaceable |
|--------------------------|-------------------------------|-------------|
| Framing                  | `TlvFrame`, `IpcMessage`       | ✅ |
| Buffering                | `StreamBuffer`                 | ✅ |
| Encoding / Decoding      | `FrameEncoder`, `FrameDecoder` | ✅ |
| Streaming                | `GenByteStream`, `Sink`        | ✅ |
| Formats                  | IPC, Parquet, CSV, TLV         | ✅ |

And more. Each layer is available as a trait + reference implementation. You can plug in your own framing, buffering, or encoding logic without rewriting the rest of the stack. 

**These slot into the Table Writers and Readers directly, so you have control of your stack and for high-performance wire formats.**

---

## Formats

- **Arrow IPC**  
  Full support for Arrow 64-byte SIMD-aligned *File* and *Stream* protocols. 
  Emit schema + dictionaries, and read back in a streaming or random-access mode.

- **TLV (Type-Length-Value)**  
  Minimal, length-delimited framing for custom data transport. Useful for control messages, telemetry, or lightweight data transfer.

- **Parquet** *(optional feature)*  
  A compact, columnar format with dictionary pages and compression (`Zstd`, `Snappy`). Lightstream implements a lean writer that keeps compatibility while avoiding external dependencies.  

- **CSV**  
  Streaming CSV readers and writers for Arrow/Minarrow `Table` and `SuperTable`. Handles headers, nulls, and custom delimiter/null-representation options.  

- **Memory Maps**  
  Read millions of rows in microseconds. Ultra-fast, zero-copy, 64-byte- aligned file ingestion for SIMD-ready analytics.  

---

## Example: Framed Stream Reader

```rust
use lightstream::models::streams::framed_byte_stream::FramedByteStream;
use lightstream::models::decoders::ipc::table_stream::TableStreamDecoder;
use lightstream::models::readers::ipc::table_stream_reader::TableStreamReader;

let framed = FramedByteStream::new(socket, TableStreamDecoder::default());
let mut reader = TableStreamReader::new(framed);

while let Some(table) = reader.next_table().await? {
    println!("Received table: {:?}", table.name);
}
```

Swap in a custom `FrameDecoder` or `StreamBuffer` when you need to change framing or buffer behaviour.

---

## Example: Custom Protocol

```rust
pub struct MyFramer;

impl FrameDecoder for MyFramer {
    type Frame = Vec<u8>;
    fn decode(&mut self, buf: &[u8]) -> DecodeResult<Self::Frame> {
        // custom framing logic
    }
}

let stream = FramedByteStream::new(socket, MyFramer);
```

This pattern gives you full control — use Lightstream’s sinks, buffers, and traits, but define your own wire format.

## Example: Write Tables

```rust
use minarrow::{arr_i32, arr_str32, FieldArray, Table};
use lightstream::io::table_writer::TableWriter;
use lightstream::enums::IPCMessageProtocol;
use tokio::fs::File;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Build a demo table
    let col1 = FieldArray::from_inner("numbers", arr_i32![1, 2, 3]);
    let col2 = FieldArray::from_inner("letters", arr_str32!["x", "y", "z"]);
    let table = Table::new("demo".into(), vec![col1, col2].into());

    // Open a file and create a writer
    let file = File::create("demo.arrow").await?;
    let schema = table.schema().to_vec();
    let mut writer = TableWriter::new(file, schema, IPCMessageProtocol::File)?;

    // Write the table and finalise
    writer.write_table(table).await?;
    writer.finish().await?;

    Ok(())
}
```

## Optional Features

- `parquet` – Parquet writer  
- `mmap` – Memory-mapped files  
- `zstd` – Zstd compression (IPC + Parquet)
- `snappy` – Snappy compression (IPC + Parquet)

---

## Use Cases

- Stream Arrow IPC tables over network sockets  
- Integrate custom binary data transport protocols
- Fast reads: Ultra-fast file ingestion with zero-copy mmap
- Analytics: SIMD-aligned Arrow IPC Writers/Readers, to avoid SIMD re-allocations downstream
- Async pipelines with backpressure-aware sinks + streams  

---

## License

This project is licensed under the MIT License. See the LICENCE file for full terms, and THIRD_PARTY_LICENSES for licences related to Apache-licensed software.

## Affiliation Notice

This project is not affiliated with Apache Arrow or the Apache Software Foundation.
It provides serialisation of the public Arrow format through a custom implementation, MinArrow, while leveraging Flatbuffers schemas from the official Arrow-RS project for schema type generation.