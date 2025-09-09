# usrbio

[![Crates.io Version](https://img.shields.io/crates/v/usrbio)](https://crates.io/crates/usrbio)

Rust bindings for the [3FS USRBIO API](https://github.com/deepseek-ai/3FS/blob/main/src/lib/api/UsrbIo.md).

Assuming you have installed the hf3fs_api_shared library.

## CLI Actions

The `usrbio` CLI tool supports several actions:

### ReadFile
Read data from a file and output to console or another file.

```bash
# Read entire file to console
cargo run --example usrbio -- --action read-file /path/to/input.txt

# Read file with offset and length to console
cargo run --example usrbio -- --action read-file --offset 100 --file-length 512 /path/to/input.txt

# Read file and save to output file
cargo run --example usrbio -- --action read-file --output-path /path/to/output.txt /path/to/input.txt
```

### WriteFile
Write data from console input or a file to a target file.

```bash
# Write from stdin to file
echo "Hello World" | cargo run --example usrbio -- --action write-file /path/to/output.txt

# Write from input file to output file
cargo run --example usrbio -- --action write-file --input-path /path/to/input.txt /path/to/output.txt

# Write with offset
cargo run --example usrbio -- --action write-file --input-path /path/to/input.txt --offset 100 /path/to/output.txt
```
