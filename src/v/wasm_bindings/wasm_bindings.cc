#include "wasm_bindings/wasm_bindings.h"

#include <seastar/core/deleter.hh>
#include <wasmtime/config.h>

#include <wasm.h>

namespace wasm_bindings {

void engine::wasm_config_deleter::operator()(wasm_config_t* cfg) {
    wasm_config_delete(cfg);
}

void engine::wasm_engine_deleter::operator()(wasm_engine_t* e) {
    wasm_engine_delete(e);
}

engine::engine(size_t stack_size)
  : _stack_size(stack_size)
  , _config(wasm_config_new(), wasm_config_deleter()) {
    wasmtime_config_dynamic_memory_guard_size_set(_config.get(), 0);
    wasmtime_config_max_wasm_stack_set(_config.get(), _stack_size);
    _engine = engine_ptr(
      wasm_engine_new_with_config(_config.get()), wasm_engine_deleter());
}

} // namespace wasm_bindings