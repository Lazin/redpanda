
rp_test(
  UNIT_TEST
  BINARY_NAME wasm_bindings_test
  SOURCES
    test_wasm_bindings.cc
  LIBRARIES 
    v::seastar_testing_main 
    v::wasm_bindings 
  ARGS "-- -c 1"
  LABELS wasm
)