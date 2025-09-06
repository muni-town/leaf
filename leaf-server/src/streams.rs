use std::{
    path::Path,
    sync::{Arc, LazyLock, Weak},
};

use anyhow::Context;
use blake3::Hash;
use leaf_stream::{Event, EventReceiver, Stream, StreamGenesis, modules::wasm::LeafWasmModule};
use tokio::sync::RwLock;
use weak_table::{WeakValueHashMap, traits::WeakElement};

use crate::storage::{GLOBAL_SQLITE_PRAGMA, STORAGE};

/// Global cache of open Leaf streams.
pub static STREAMS: LazyLock<Streams> = LazyLock::new(Streams::default);

#[derive(Debug, Clone)]
pub struct StreamHandle(Arc<StreamHandleInner>);

impl StreamHandle {
    pub fn id(&self) -> Hash {
        self.0.id
    }

    /// Subscribe to events that are sent over this stream.
    pub async fn subscribe(&self, requesting_user: &str) -> EventReceiver {
        self.0.stream.read().await.subscribe(requesting_user).await
    }

    pub async fn handle_event(&self, user: String, payload: Vec<u8>) -> anyhow::Result<()> {
        let mut stream = self.0.stream.write().await;
        let new_module = stream.handle_event(user, payload).await?;

        if let Some(module_id) = new_module {
            let data_dir = STORAGE.data_dir()?;
            let stream_dir = data_dir.join("streams").join(self.0.id.to_hex().as_str());
            let (module, module_db) = load_module(&stream_dir, module_id).await?;
            stream.provide_module(module, module_db).await?;
            STORAGE
                .update_stream_current_module(self.0.id, module_id)
                .await
                .context("error updating current module for stream")?;
        }

        Ok(())
    }

    pub async fn fetch_events(
        &self,
        requesting_user: &str,
        offset: u64,
        limit: u64,
    ) -> anyhow::Result<Vec<Event>> {
        let events = self
            .0
            .stream
            .read()
            .await
            .fetch_events(requesting_user, offset, limit)
            .await?;
        Ok(events)
    }
}

#[derive(Debug, Clone)]
pub struct WeakStreamHandle(Weak<StreamHandleInner>);

#[derive(Debug)]
pub struct StreamHandleInner {
    id: Hash,
    stream: RwLock<Stream>,
}

#[derive(Default)]
pub struct Streams {
    streams: RwLock<WeakValueHashMap<Hash, WeakStreamHandle>>,
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
        stream_db
            .execute_batch(GLOBAL_SQLITE_PRAGMA)
            .await?;

        // Open the stream
        let mut stream = leaf_stream::Stream::open(genesis, stream_db).await?;
        // Spawn background worker task for the stream
        stream.creat_worker_task().map(tokio::spawn);

        // Load the stream's module and it's database
        if let Some(module_id) = stream.needs_module().await {
            let (module, module_db) = load_module(&stream_dir, module_id).await?;
            stream.provide_module(module, module_db).await?;
        }

        // Store the stream handle in the cache
        let handle = StreamHandle(Arc::new(StreamHandleInner {
            id: stream.id(),
            stream: RwLock::new(stream),
        }));
        self.streams.write().await.insert(id, handle.clone());

        // Return the stream handle
        Ok(handle)
    }
}

async fn load_module(
    stream_dir: &Path,
    module_id: Hash,
) -> anyhow::Result<(Arc<LeafWasmModule>, libsql::Connection)> {
    let Some(module) = STORAGE.get_wasm_module(module_id).await? else {
        anyhow::bail!("Could not load module needed by stream: {}", module_id);
    };
    let module_db_path = stream_dir.join(format!("module_{}.db", module_id.to_hex().as_str()));
    let module_db = libsql::Builder::new_local(module_db_path)
        .build()
        .await
        .context("error opening module db")?
        .connect()?;
    module_db
        .execute_batch(GLOBAL_SQLITE_PRAGMA)
        .await?;
    Ok((module, module_db))
}

impl std::hash::Hash for StreamHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.id.hash(state);
    }
}
impl std::cmp::Eq for StreamHandle {}
impl std::cmp::PartialEq for StreamHandle {
    fn eq(&self, other: &Self) -> bool {
        self.0.id == other.0.id
    }
}

impl WeakElement for WeakStreamHandle {
    type Strong = StreamHandle;
    fn new(view: &Self::Strong) -> Self {
        WeakStreamHandle(Arc::downgrade(&view.0))
    }
    fn view(&self) -> Option<Self::Strong> {
        self.0.upgrade().map(StreamHandle)
    }
}
