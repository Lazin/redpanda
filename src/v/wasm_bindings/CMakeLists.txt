# TODO: remove this when build is properly configured
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/v/wasm/wasmtime/include)

v_cc_library(
  NAME wasm_bindings
  SRCS
    wasm_bindings.cc
  DEPS
    Seastar::seastar
    libwasmtime.a
    )
    

add_subdirectory(tests)