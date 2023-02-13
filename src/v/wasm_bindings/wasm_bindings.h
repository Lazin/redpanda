#include "seastarx.h"

#include <seastar/core/deleter.hh>

#include <wasmtime/engine.h>

#include <memory>
// TODO: remove these headers and use fwd declarations;
#include <wasm.h>
#include <wasmtime.h>

namespace wasm_bindings {

class engine {
    struct wasm_config_deleter {
        void operator()(wasm_config_t* cfg);
    };

    struct wasm_engine_deleter {
        void operator()(wasm_engine_t* e);
    };
    using config_ptr = std::unique_ptr<wasm_config_t, wasm_config_deleter>;
    using engine_ptr = std::unique_ptr<wasm_engine_t, wasm_engine_deleter>;

public:
    explicit engine(size_t stack_size);

private:
    const size_t _stack_size;
    engine_ptr _engine;
    config_ptr _config;
};

} // namespace wasm_bindings