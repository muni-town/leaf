use std::sync::LazyLock;

use wasmer::{Module, Store};

pub static STORE: LazyLock<Store> = LazyLock::new(Store::default);

pub fn validate_wasm(bytes: &[u8]) -> anyhow::Result<()> {
    Ok(Module::validate(&STORE, bytes)?)
}
