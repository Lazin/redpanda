#include "wasm_bindings/wasm_bindings.h"

#include <boost/test/unit_test.hpp>
#include <units.h>

using namespace wasm_bindings;

BOOST_AUTO_TEST_CASE(wasm_bindings_create_engine) {
    const size_t stack_size = 32_KiB;
    engine ee(stack_size);
}