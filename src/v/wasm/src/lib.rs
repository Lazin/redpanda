use anyhow::Result;
use core::task::Poll;
use cxx::let_cxx_string;
use futures::future::BoxFuture;
use futures::future::Future;

pub struct Engine {
    engine: wasmtime::Engine,
    use_fuel: bool,
}

fn create_engine(max_stack_size: usize, use_fuel: bool) -> Box<Engine> {
    let_cxx_string!(log_msg = format!("create_engine called, max_stack_size={}", max_stack_size));
    ffi::log_info(&log_msg);
    let mut config = wasmtime::Config::new();
    // Use async mode with epoch interruption
    config.async_support(true);
    if use_fuel {
        config.consume_fuel(true);
    } else {
        config.epoch_interruption(true);
    }
    // Do not reserve memory in advance since we're aiming at running
    // a lot of wasm function preemptively on a single shard.
    config.static_memory_maximum_size(0);
    config.dynamic_memory_reserved_for_growth(0);
    config.dynamic_memory_guard_size(0); // TODO: use some meaningful value
    config.max_wasm_stack(max_stack_size);
    config.async_stack_size(max_stack_size);
    // TODO: use 'with_host_memory' and provide custom allocator that taps
    // into seastar allocator and adds some metrics on top.

    let result = wasmtime::Engine::new(&config).unwrap();
    Box::new(Engine {
        engine: result,
        use_fuel: use_fuel,
    })
}

impl Engine {
    fn increment_epoch(&mut self) {
        self.engine.increment_epoch();
    }
}

pub struct Module {
    module: wasmtime::Module,
    binary: Vec<u8>,
}

// Create precompiled module
fn create_module(engine: &mut Engine, wasm_func: &str) -> Box<Module> {
    let_cxx_string!(log_msg = format!("create_module called, wat={}", wasm_func));
    ffi::log_info(&log_msg);
    let bin = engine
        .engine
        .precompile_module(wasm_func.as_bytes())
        .unwrap();
    let module = unsafe {
        wasmtime::Module::deserialize(&engine.engine, &bin).unwrap()
        // FIXME: handle error properly
    };
    Box::new(Module {
        binary: bin,
        module: module,
    })
}

fn create_module_from_file(engine: &Engine, wasm_file: &str) -> Box<Module> {
    let_cxx_string!(log_msg = format!("create_module_from_file called, wat={}", wasm_file));
    ffi::log_info(&log_msg);
    let module = wasmtime::Module::from_file(&engine.engine, wasm_file).unwrap();
    Box::new(Module {
        module: module,
        binary: Vec::new(),
    })
}

pub struct Store {
    store: wasmtime::Store<wasmtime_wasi::WasiCtx>,
}

fn create_store(engine: &mut Engine, fuel: u64, yield_fuel: u64) -> Box<Store> {
    let_cxx_string!(
        log_msg = format!(
            "create_store called, use_fuel={}, total={}, yield={}",
            engine.use_fuel, fuel, yield_fuel
        )
    );
    ffi::log_info(&log_msg);
    let ctx = wasmtime_wasi::WasiCtxBuilder::new()
        .inherit_stdio()
        .inherit_args()
        .unwrap()
        .build();
    let mut store = wasmtime::Store::new(&engine.engine, ctx);
    if !engine.use_fuel {
        store.epoch_deadline_async_yield_and_update(1);
        store.set_epoch_deadline(0);
    } else {
        store.out_of_fuel_async_yield(fuel, yield_fuel);
    }
    Box::new(Store { store: store })
}

impl Store {
    fn add_fuel(&mut self, injected_fuel: u64) {
        self.store.add_fuel(injected_fuel).unwrap();
    }
}

pub struct Instance {
    instance: wasmtime::Instance,
}

fn create_instance(engine: &Engine, module: &Module, store: &mut Store) -> Box<Instance> {
    let_cxx_string!(log_msg = format!("create_instance called"));
    ffi::log_info(&log_msg);
    let mut linker = wasmtime::Linker::new(&engine.engine);
    wasmtime_wasi::add_to_linker(&mut linker, |s| s).unwrap();
    //
    let mut async_fut = Box::pin(linker.instantiate_async(&mut store.store, &module.module));
    let mut ctx = core::task::Context::from_waker(futures::task::noop_waker_ref());
    loop {
        match async_fut.as_mut().poll(&mut ctx) {
            Poll::Pending => {}
            Poll::Ready(inst) => {
                return Box::new(Instance {
                    instance: inst.unwrap(),
                });
            }
        }
    }
}

pub struct Function {
    func: wasmtime::TypedFunc<(), ()>,
}

fn create_function(instance: &Instance, store: &mut Store, fname: &str) -> Box<Function> {
    let exp = instance
        .instance
        .get_typed_func::<(), ()>(&mut store.store, fname)
        .unwrap();
    Box::new(Function { func: exp })
}

pub struct Fut<'a> {
    fut: BoxFuture<'a, Result<()>>,
}

impl<'a> Fut<'a> {
    fn resume(&mut self) -> bool {
        match self.fut.as_mut().poll(&mut core::task::Context::from_waker(
            futures::task::noop_waker_ref(),
        )) {
            Poll::Pending => false,
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(e)) => panic!("poll failed: {}", e),
        }
    }
}

fn get_void_func_future<'a>(store: &'a mut Store, func: &'a Function) -> Box<Fut<'a>> {
    Box::new(Fut {
        fut: Box::pin(func.func.call_async(&mut store.store, ())),
    })
}

#[cxx::bridge(namespace = "wasm")]
mod ffi {
    extern "Rust" {
        type Engine;
        fn create_engine(max_stack_size: usize, use_fuel: bool) -> Box<Engine>;
        fn increment_epoch(self: &mut Engine);

        type Instance;
        fn create_instance(engine: &Engine, module: &Module, store: &mut Store) -> Box<Instance>;

        type Module;
        fn create_module(engine: &mut Engine, wasm_func: &str) -> Box<Module>;
        fn create_module_from_file(engine: &Engine, wasm_file: &str) -> Box<Module>;

        type Store;
        fn create_store(engine: &mut Engine, fuel: u64, yield_fuel: u64) -> Box<Store>;
        fn add_fuel(self: &mut Store, injected_fuel: u64);

        type Function;

        fn create_function(instance: &Instance, store: &mut Store, fname: &str) -> Box<Function>;

        type Fut<'a>;
        unsafe fn get_void_func_future<'a>(
            store: &'a mut Store,
            func: &'a Function,
        ) -> Box<Fut<'a>>;
        unsafe fn resume(self: &mut Fut<'_>) -> bool;
    }

    unsafe extern "C++" {
        include!("wasm/logger.h");
        fn log_info(msg: &CxxString);
    }
}
