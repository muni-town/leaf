use std::{
    pin::Pin,
    sync::{Arc, LazyLock},
};

use blake3::Hash;
use wasmtime::{Config, Engine, Store, Val};

use crate::{InboundFilterResponse, LeafModule, ModuleUpdate};

pub static ENGINE: LazyLock<Engine> =
    LazyLock::new(|| Engine::new(Config::new().async_support(true)).unwrap());

#[derive(Debug)]
pub struct LeafWasmModule {
    id: Hash,
    module: wasmtime::Module,
    linker: Arc<wasmtime::Linker<libsql::Connection>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ModuleError {
    #[error("Error with WASM module interface: {0}")]
    InterfaceError(#[from] WasmInterfaceError),
    #[error("WASM error: {0}")]
    WasmError(#[from] wasmtime::Error),
}

#[derive(thiserror::Error, Debug)]
pub enum WasmInterfaceError {}

impl LeafWasmModule {
    pub fn new(wasm: &[u8]) -> Result<Self, ModuleError> {
        let id = blake3::hash(wasm);
        let module = wasmtime::Module::new(&ENGINE, wasm)?;
        let mut linker = wasmtime::Linker::<libsql::Connection>::new(&ENGINE);
        linker.func_wrap_async("host", "query", |caller, param: ()| Box::new(async move {}))?;

        Self::validate_module_interface(&module)?;

        Ok(Self {
            id,
            module,
            linker: Arc::new(linker),
        })
    }

    fn validate_module_interface(_w_module: &wasmtime::Module) -> Result<(), WasmInterfaceError> {
        Ok(())
    }
}

impl LeafModule for LeafWasmModule {
    fn id(&self) -> blake3::Hash {
        self.id
    }

    fn filter_inbound(
        &mut self,
        payload: Vec<u8>,
        params: Vec<u8>,
        user: String,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<InboundFilterResponse>>>> {
        let module = self.module.clone();
        let linker = self.linker.clone();

        Box::pin(async move {
            let mut store = Store::new(&ENGINE, db);
            let instance = linker.instantiate_async(&mut store, &module).await?;
            let Some(memory) = instance.get_memory(&mut store, "memory") else {
                anyhow::bail!("Could not load wasm memory.");
            };
            let Some(malloc) = instance.get_func(&mut store, "malloc") else {
                anyhow::bail!("WASM module does not export `malloc` function");
            };
            let Some(filter_inbound) = instance.get_func(&mut store, "filter_inbound") else {
                return Ok(InboundFilterResponse::Accept);
            };

            // Allocate the payload
            let payload_len = payload.len();
            let mut results = [Val::null_any_ref()];
            malloc
                .call_async(
                    &mut store,
                    &[Val::I32(payload_len as i32), Val::I32(1)],
                    &mut results,
                )
                .await?;
            let Some(payload_ptr) = results[0].i32() else {
                anyhow::bail!("Invalid return type from malloc");
            };
            memory.write(&mut store, payload_ptr as usize, &payload)?;

            // Allocate the params
            let params_len = params.len();
            let mut results = [Val::null_any_ref()];
            malloc
                .call_async(
                    &mut store,
                    &[Val::I32(params_len as i32), Val::I32(1)],
                    &mut results,
                )
                .await?;
            let Some(params_ptr) = results[0].i32() else {
                anyhow::bail!("Invalid return type from malloc");
            };
            memory.write(&mut store, params_ptr as usize, &params)?;

            // Allocate the user
            let user_len = user.len();
            let mut results = [Val::null_any_ref()];
            malloc
                .call_async(
                    &mut store,
                    &[Val::I32(user_len as i32), Val::I32(1)],
                    &mut results,
                )
                .await?;
            let Some(user_ptr) = results[0].i32() else {
                anyhow::bail!("Invalid return type from malloc");
            };
            memory.write(&mut store, user_ptr as usize, user.as_bytes())?;

            let mut results = Vec::with_capacity(1);
            filter_inbound
                .call_async(&mut store, &[], &mut results)
                .await?;

            Ok(InboundFilterResponse::Accept)
        })
    }

    fn filter_outbound(
        &mut self,
        payload: Vec<u8>,
        params: Vec<u8>,
        user: String,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>>>> {
        Box::pin(async move {
            //
            Ok(false)
        })
    }

    fn process_event(
        &mut self,
        payload: Vec<u8>,
        params: Vec<u8>,
        user: String,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ModuleUpdate>>>> {
        Box::pin(async move {
            //
            Ok(ModuleUpdate {
                new_module: None,
                new_params: None,
            })
        })
    }
}
