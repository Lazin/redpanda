rp_test(
  UNIT_TEST
  BINARY_NAME wasm_bindings_test
  SOURCES
    wasm_bindings_test.cc
  LIBRARIES v::seastar_testing_main v::wasm_bindings
  ARGS "-- -c 1"
  LABELS wasm_bindings
)