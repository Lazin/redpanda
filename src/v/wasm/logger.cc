#include "wasm/logger.h"

#include "vlog.h"

#include "seastarx.h"

#include <seastar/util/log.hh>

static seastar::logger wasm_logger("wasmtime");

namespace wasm{
void log_info(const std::string& message) {
    vlog(wasm_logger.info, "{}", message);
}
}