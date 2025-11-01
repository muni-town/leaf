#![allow(unused)] // Temporarily allow unused code since we are still working on this

use std::sync::{Arc, LazyLock};

use blake3::Hash;
use leaf_stream_types::LeafModuleDef;
use wasmtime::{Config, Engine};

pub static ENGINE: LazyLock<Engine> =
    LazyLock::new(|| Engine::new(Config::new().async_support(true)).unwrap());

pub struct LeafModule {
    id: Hash,
    def: LeafModuleDef,
    wasm: Option<LeafModuleWasm>,
}

#[derive(Debug)]
pub struct LeafModuleWasm {
    id: Hash,
    module: wasmtime::Module,
    linker: Arc<wasmtime::Linker<libsql::Connection>>,
}

#[derive(Debug, thiserror::Error)]
pub enum LeafModuleError {
    #[error("The WASM module provided ( {actual:?} ) did not have the expected ID: {expected:?}")]
    WasmModuleIdMismatch {
        expected: Option<Hash>,
        actual: Option<Hash>,
    },
    #[error("Wasm error: {0}")]
    Wasmtime(#[from] wasmtime::Error),
}

impl LeafModule {
    pub fn id(&self) -> Hash {
        self.id
    }

    pub fn def(&self) -> &LeafModuleDef {
        &self.def
    }

    pub fn new(def: LeafModuleDef, wasm: Option<&[u8]>) -> Result<Self, LeafModuleError> {
        let id = def.module_id_and_bytes().0;
        let wasm = if let Some(wasm) = wasm {
            let wasm = LeafModuleWasm::load(wasm)?;
            let expected_id = def.wasm_module.map(Hash::from_bytes);
            let Some(expected_id) = expected_id else {
                return Err(LeafModuleError::WasmModuleIdMismatch {
                    expected: expected_id,
                    actual: Some(wasm.id),
                });
            };
            if wasm.id != expected_id {
                return Err(LeafModuleError::WasmModuleIdMismatch {
                    expected: Some(expected_id),
                    actual: Some(wasm.id),
                });
            }
            Some(wasm)
        } else {
            if let Some(wasm) = wasm {
                let wasm_id = blake3::hash(wasm);
                return Err(LeafModuleError::WasmModuleIdMismatch {
                    expected: None,
                    actual: Some(wasm_id),
                });
            }
            None
        };
        Ok(LeafModule { id, def, wasm })
    }
}

impl LeafModuleWasm {
    pub fn load(wasm: &[u8]) -> wasmtime::Result<Self> {
        let id = blake3::hash(wasm);
        let module = wasmtime::Module::new(&ENGINE, wasm)?;
        let linker = wasmtime::Linker::<libsql::Connection>::new(&ENGINE);
        Ok(Self {
            id,
            module,
            linker: Arc::new(linker),
        })
    }
}
