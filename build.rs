use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rustc-link-lib=dylib=hf3fs_api_shared");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindgen::Builder::default()
        .header("include/hf3fs_usrbio.h")
        .derive_copy(true)
        .derive_debug(true)
        .derive_default(true)
        .generate_comments(false)
        .prepend_enum_name(false)
        .formatter(bindgen::Formatter::Rustfmt)
        .size_t_is_usize(true)
        .translate_enum_integer_types(true)
        .layout_tests(false)
        .allowlist_type("timespec")
        .allowlist_item("hf3fs.*")
        .allowlist_item("HF3FS.*")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Unable to write bindings!");
}
