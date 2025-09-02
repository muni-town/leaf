use std::sync::{Arc, LazyLock, Weak};

use blake3::Hash;
use tokio::sync::RwLock;
use wasmer::{Module, Store};
use weak_table::WeakValueHashMap;

use crate::storage::STORAGE;

pub static STORE: LazyLock<Store> = LazyLock::new(Store::default);

/// Global cache of compiled WASM modules.
pub static MODULES: LazyLock<Modules> = LazyLock::new(Modules::default);

#[derive(Default)]
pub struct Modules {
    cache: RwLock<WeakValueHashMap<Hash, Weak<wasmer::Module>>>,
}

impl Modules {
    pub async fn load(&self, hash: Hash) -> anyhow::Result<Option<Arc<wasmer::Module>>> {
        {
            let cache = self.cache.read().await;
            if let Some(module) = cache.get(&hash) {
                return Ok(Some(module));
            }
        }
        let Some(module) = STORAGE.get_module(hash).await? else {
            return Ok(None);
        };
        let module = Arc::new(Module::new(&STORE, module)?);
        self.cache.write().await.insert(hash, module.clone());
        Ok(Some(module))
    }
}

pub fn validate_wasm(bytes: &[u8]) -> anyhow::Result<()> {
    tracing::warn!(
        "TODO: we are not validating the WASM exports / imports match our expectations yet."
    );
    Ok(Module::validate(&STORE, bytes)?)
}
