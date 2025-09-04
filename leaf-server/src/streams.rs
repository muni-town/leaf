use std::sync::{Arc, LazyLock, Weak};

use anyhow::Context;
use blake3::Hash;
use leaf_stream::{Stream, StreamGenesis, modules::wasm::LeafWasmModule};
use tokio::sync::RwLock;
use weak_table::WeakValueHashMap;

use crate::storage::STORAGE;

/// Global cache of open Leaf streams.
pub static STREAMS: LazyLock<Streams> = LazyLock::new(Streams::default);

pub type StreamHandle = Arc<Stream>;

#[derive(Default)]
pub struct Streams {
    streams: RwLock<WeakValueHashMap<Hash, Weak<Stream>>>,
}

impl Streams {
    pub async fn load(&self, genesis: StreamGenesis) -> anyhow::Result<StreamHandle> {
        let id = genesis.get_stream_id_and_bytes().0;

        // Return the stream from the streams if it is already open
        {
            let streams = self.streams.read().await;
            if let Some(module) = streams.get(&id) {
                return Ok(module);
            }
        }

        // Make sure the stream data dir exists
        let data_dir = STORAGE.data_dir()?;
        let stream_dir = data_dir.join("streams").join(id.to_hex().as_str());
        tokio::fs::create_dir_all(&stream_dir).await?;

        // Open the stream's database
        let stream_db_path = stream_dir.join("stream.db");
        let stream_db = libsql::Builder::new_local(&stream_db_path)
            .build()
            .await
            .context("error opening stream db")?
            .connect()?;

        // Open the stream
        let mut stream = leaf_stream::Stream::open(genesis, stream_db).await?;

        // Load the stream's module and it's database
        if let Some(module_id) = stream.needs_module() {
            let Some(module) = STORAGE.get_module(module_id).await? else {
                anyhow::bail!("Could not load module needed by stream: {}", module_id);
            };
            let module_db_path =
                stream_dir.join(format!("module_{}.db", module_id.to_hex().as_str()));
            let module_db = libsql::Builder::new_local(module_db_path)
                .build()
                .await
                .context("error opening module db")?
                .connect()?;
            stream.provide_module(Box::new(LeafWasmModule::new(&module)?), module_db)?;
        }

        // Store the stream handle in the cache
        let stream = Arc::new(stream);
        self.streams.write().await.insert(id, stream.clone());

        // Return the stream handle
        Ok(stream)
    }
}
