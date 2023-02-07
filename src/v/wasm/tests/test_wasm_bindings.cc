#include "vlog.h"
#include "wasm/target/cxxbridge/wasm/src/lib.rs.h"
#include "wasm/wasm_bindings.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>
#include <seastar/util/log.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <exception>
#include <thread>

static seastar::logger test_log("test_log");
using namespace std::chrono_literals;

void run_wasm_function(bool run_epoch_termination_thread, bool use_fuel) {
    auto engine = wasm::create_engine(128 * 1024, use_fuel);
    auto mod = wasm::create_module_from_file(
      *engine,
      "src/v/wasm/tests/wasm_module/target/wasm32-wasi/release/"
      "wasm_module.wasm");
    uint64_t fuel = 100000000;
    // NOTE: fuel is ignored if 'use_fuel' is false
    auto store = wasm::create_store(*engine, fuel, fuel);
    auto instance = wasm::create_instance(*engine, *mod, *store);
    auto func = wasm::create_function(*instance, *store, "test_function");

    vlog(test_log.info, "Running wasm function");
    auto fut = wasm::get_void_func_future(*store, *func);

    std::atomic<bool> flag{false};
    std::thread epoch_thread([&flag, &engine, run_epoch_termination_thread] {
        if (!run_epoch_termination_thread) {
            return;
        }
        while (!flag) {
            std::this_thread::sleep_for(10ms);
            engine->increment_epoch();
        }
    });

    auto deadline = seastar::lowres_clock::now() + 100s;
    while (true) {
        try {
            vlog(test_log.info, "Resuming execution");
            if (!fut->resume()) {
                auto now = seastar::lowres_clock::now();
                if (now > deadline) {
                    vlog(test_log.info, "Force quit");
                    break;
                } else {
                    seastar::maybe_yield().get();
                    if (use_fuel) {
                        vlog(test_log.info, "Add fuel");
                        store->add_fuel(fuel);
                    } else {
                        vlog(test_log.info, "Yield");
                    }
                }
            } else {
                vlog(test_log.info, "Finished");
                break;
            }
        } catch (...) {
            vlog(test_log.error, "WASM error: {}", std::current_exception());
        }
    }

    flag = true;
    epoch_thread.join();
}

SEASTAR_THREAD_TEST_CASE(wasm_bindings_test_epoch_1) { run_wasm_function(false, false); }

SEASTAR_THREAD_TEST_CASE(wasm_bindings_test_epoch_2) { run_wasm_function(true, false); }

SEASTAR_THREAD_TEST_CASE(wasm_bindings_test_fuel) { run_wasm_function(false, true); }
