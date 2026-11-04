# usrbio

[![Crates.io Version](https://img.shields.io/crates/v/usrbio)](https://crates.io/crates/usrbio)

Rust bindings for the [3FS USRBIO API](https://github.com/deepseek-ai/3FS/blob/main/src/lib/api/UsrbIo.md).
The crate provides registered files, shared-memory I/O rings, batched reads and
writes, and background read workers.

## Requirements

- Linux with a mounted 3FS filesystem for I/O.
- A Rust toolchain and `libclang` for generating bindings at build time.
- A compatible `libhf3fs_api_shared.so`, available to both the linker and the
  runtime loader. The library must provide the APIs in
  [`include/hf3fs_usrbio.h`](https://github.com/SF-Zhou/usrbio/blob/main/include/hf3fs_usrbio.h), including `hf3fs_iorcreate4`
  and `hf3fs_iovunlink`.

The 3FS library is installed separately. For a nonstandard installation directory,
add it to `LIBRARY_PATH` when building and `LD_LIBRARY_PATH` when running.

## Usage

Add the crate to your project:

```toml
[dependencies]
usrbio = "0.1"
```

Read up to 4 KiB from an existing file on your 3FS mount:

```rust,no_run
use usrbio::{File, Ring, RingConfig};

fn main() -> usrbio::Result<()> {
    let file = File::open("/mnt/3fs/example.txt")?;
    let mut ring = Ring::create(&RingConfig::default(), file.mount_point())?;

    let data = ring.read(&file, 0, 4096)?;
    println!("{}", String::from_utf8_lossy(data));
    Ok(())
}
```

`Ring` performs blocking I/O. Read slices borrow the ring's buffer;
copy them if they must outlive that borrow. Each ring serves files on one mount,
and each batch must fit its configured entry count and buffer size. Batch results
contain a byte count or a negative error code for each job, so check each result
even when the batch call returns `Ok`.

For writes, open the file with `std::fs::OpenOptions` and the `Open3fsFile`
extension trait. Use `ReadWorker` or `ReadWorkers` for reads on background threads.
See the [API documentation](https://docs.rs/usrbio) for details.

## CLI

```sh
cargo install usrbio --features bin
```

Or build from source:

```sh
cargo build --release --features bin
```

Run a read benchmark against existing, nonempty files on the same 3FS mount:

```sh
usrbio --action bench --threads 4 --bs 512k --iodepth 16 --duration 10s /mnt/3fs/data
usrbio --help
```

When building from source, use `./target/release/usrbio`. Other actions are
`check` (read files and print CRC32C checksums), `gen-files` (create or overwrite
test files), and `write-file` (write a file or stdin at an offset).

## Development

```sh
cargo check --all-targets --all-features
cargo test --doc
cargo doc --no-deps
```

The integration tests require a writable 3FS directory and create or overwrite
test files there. Set `USRBIO_3FS_TEST_DIR` **at compile time** to run them:

```sh
USRBIO_3FS_TEST_DIR=/mnt/3fs/usrbio-tests cargo test --test test_file
```

Without that variable, the integration tests return early without exercising I/O.

## License

Licensed under [MIT](https://github.com/SF-Zhou/usrbio/blob/main/LICENSE-MIT) or
[Apache-2.0](https://github.com/SF-Zhou/usrbio/blob/main/LICENSE-APACHE), at your option.
The bundled 3FS header is covered by [LICENSE-3FS](https://github.com/SF-Zhou/usrbio/blob/main/LICENSE-3FS).
