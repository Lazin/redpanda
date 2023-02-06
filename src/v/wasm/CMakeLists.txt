set(CARGO_MANIFEST ${CMAKE_SOURCE_DIR}/src/v/wasm/Cargo.toml)
set(CARGO_TARGET_DIR ${CMAKE_SOURCE_DIR}/src/v/wasm/target)

set(WASMTIME_BINDINGS_SOURCE_RS ${CMAKE_SOURCE_DIR}/src/v/wasm/src/lib.rs)
set(WASMTIME_BINDINGS_SOURCE_CC ${CARGO_TARGET_DIR}/cxxbridge/wasm/src/lib.rs.cc)

# TODO: support debug bulid
set(WASMTIME_BINDINGS_LIB ${CARGO_TARGET_DIR}/release/libwasm.a)

# TODO: put generated stuff into corresponding data dir
#       using CARGO_TARGET_DIR env var
add_custom_command(
        OUTPUT ${WASMTIME_BINDINGS_SOURCE_CC} ${WASMTIME_BINDINGS_LIB}
        COMMAND cargo build --release --manifest-path ${CARGO_MANIFEST}
        DEPENDS ${WASMTIME_BINDINGS_SOURCE_RS}
        USES_TERMINAL
        COMMENT "Running cargo build"
)

v_cc_library(
  NAME wasm_bindings
  SRCS
    ${WASMTIME_BINDINGS_SOURCE_CC}
  DEPS
    ${WASMTIME_BINDINGS_LIB}
    Seastar::seastar
    v::bytes
    v::model)

add_subdirectory(tests)