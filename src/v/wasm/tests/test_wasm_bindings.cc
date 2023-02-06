#include "wasm/target/cxxbridge/wasm/src/lib.rs.h"
#include "wasm/wasm_bindings.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

SEASTAR_THREAD_TEST_CASE(wasm_bindings_test_1) {
    wasm::hello_from_rust();
}
