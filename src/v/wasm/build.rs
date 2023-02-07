#[allow(unused_must_use)]
fn main() {
    cxx_build::bridge("src/lib.rs")
        .flag_if_supported("-std=c++20")
        .flag_if_supported("-stdlib=libc++")
        .compile("wasm-bindings");
    println!("cargo:rerun-if-changed=src/lib.rs");
}

