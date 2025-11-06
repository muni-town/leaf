use std::{
    path::Path,
    sync::{Arc, LazyLock, Weak},
};

use anyhow::Context;
use blake3::Hash;
use leaf_stream::{LeafModule, Stream, StreamGenesis, types::LeafModuleDef};
use tokio::sync::RwLock;
use weak_table::WeakValueHashMap;

use crate::storage::{GLOBAL_SQLITE_PRAGMA, STORAGE};

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
        stream_db.execute_batch(GLOBAL_SQLITE_PRAGMA).await?;

        // Open the stream
        let stream = leaf_stream::Stream::open(genesis, stream_db).await?;
        // Spawn background worker task for the stream
        stream.create_worker_task().await.map(tokio::spawn);

        // Load the stream's module and it's database
        if let Some(module_def) = stream.needs_module().await {
            STORAGE
                .update_stream_wasm_module(id, module_def.wasm_module.map(Hash::from_bytes))
                .await?;
            let (module, module_db) = load_module(&stream_dir, module_def).await?;
            stream.provide_module(module, module_db).await?;
        }

        // Spawn a task that will watch the latest event in the stream and update the main database
        let latest_event_rx = stream.subscribe_latest_event().await;
        tokio::spawn(async move {
            while let Ok(latest_event) = latest_event_rx.recv().await {
                if let Err(e) = STORAGE.set_latest_event(id, latest_event).await {
                    tracing::error!("Error updating latest event for stream in database: {e}");
                }
            }
        });

        // Store the stream handle in the cache
        let handle = Arc::new(stream);
        self.streams.write().await.insert(id, handle.clone());

        // Return the stream handle
        Ok(handle)
    }

    pub async fn update_module(
        &self,
        stream: Arc<Stream>,
        module_def: LeafModuleDef,
    ) -> anyhow::Result<()> {
        let data_dir = STORAGE.data_dir()?;
        let stream_dir = data_dir.join("streams").join(stream.id().to_hex().as_str());

        let wasm_hash = module_def.wasm_module.map(Hash::from_bytes);
        if let Some(hash) = wasm_hash
            && !STORAGE.has_wasm_blob(hash).await?
        {
            anyhow::bail!("WASM module not found and must be uploaded first: {hash}");
        }
        stream.raw_set_module(module_def.clone()).await?;

        let (module, db) = load_module(&stream_dir, module_def).await?;
        stream.provide_module(module, db).await?;

        STORAGE
            .update_stream_wasm_module(stream.id(), wasm_hash)
            .await?;

        Ok(())
    }
}

pub async fn load_module(
    stream_dir: &Path,
    module_def: LeafModuleDef,
) -> anyhow::Result<(Arc<LeafModule>, libsql::Connection)> {
    let module_id = module_def.module_id_and_bytes().0;
    let Some(module) = STORAGE.get_module(module_def).await? else {
        anyhow::bail!("Could not load module needed by stream: {}", module_id);
    };
    let module_db_path = stream_dir.join(format!("module_{}.db", module_id.to_hex().as_str()));
    let module_db = libsql::Builder::new_local(module_db_path)
        .build()
        .await
        .context("error opening module db")?
        .connect()?;

    // Set our standard pragmas
    module_db.execute_batch(GLOBAL_SQLITE_PRAGMA).await?;

    // Attach the stream DB to the module DB
    module_db
        .execute(
            "attach ? as events",
            [stream_dir.join("stream.db").to_string_lossy().to_string()],
        )
        .await?;

    Ok((module, module_db))
}
