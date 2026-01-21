use std::{
    path::Path,
    sync::{Arc, LazyLock, Weak},
};

use anyhow::Context;
use leaf_stream::{LeafModule, Stream, atproto_plc::Did, dasl::cid::Cid};
use tokio::sync::RwLock;
use weak_table::WeakValueHashMap;

use crate::storage::{GLOBAL_SQLITE_PRAGMA, STORAGE};

/// Global cache of open Leaf streams.
pub static STREAMS: LazyLock<Streams> = LazyLock::new(Streams::default);

pub type StreamHandle = Arc<Stream>;

#[derive(Default)]
pub struct Streams {
    streams: RwLock<WeakValueHashMap<Did, Weak<Stream>>>,
}

impl Streams {
    #[tracing::instrument(skip(self))]
    pub async fn load(&self, id: Did) -> anyhow::Result<StreamHandle> {
        // Return the stream from the streams if it is already open
        {
            let streams = self.streams.read().await;
            if let Some(module) = streams.get(&id) {
                return Ok(module);
            }
        }

        // Make sure the stream data dir exists
        let data_dir = STORAGE.data_dir()?;
        let stream_dir = data_dir.join("streams").join(id.as_str());
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
        let stream = leaf_stream::Stream::open(id.clone(), stream_db).await?;
        // Spawn background worker task for the stream
        stream.create_worker_task().await.map(tokio::spawn);

        // Load the stream's module and it's database
        if let Some(Some(module_cid)) = stream.needs_module().await {
            STORAGE.update_stream_module(id.clone(), module_cid).await?;
            let (module, module_db) = load_module(&stream_dir, module_cid).await?;
            if let Err(e) = stream.provide_module(module, module_db).await {
                tracing::warn!("Error providing stream module when opening stream: {e}");
            }
        }

        // Spawn a task that will watch the latest event in the stream and update the main database
        let latest_event_rx = stream.subscribe_latest_event().await;
        let id_ = id.clone();
        tokio::spawn(async move {
            while let Ok(latest_event) = latest_event_rx.recv().await {
                if let Err(e) = STORAGE.set_latest_event(id_.clone(), latest_event).await {
                    tracing::error!("Error updating latest event for stream in database: {e}");
                }
            }
        });

        // Store the stream handle in the cache
        let handle = Arc::new(stream);
        self.streams
            .write()
            .await
            .insert(id.clone(), handle.clone());

        // Return the stream handle
        Ok(handle)
    }

    pub async fn update_module(&self, stream: Arc<Stream>, module_cid: Cid) -> anyhow::Result<()> {
        let data_dir = STORAGE.data_dir()?;
        let stream_dir = data_dir.join("streams").join(stream.id().as_str());

        stream.raw_set_module(module_cid).await?;
        let (module, db) = load_module(&stream_dir, module_cid).await?;
        stream.provide_module(module, db).await?;
        STORAGE
            .update_stream_module(stream.id().clone(), module_cid)
            .await?;

        Ok(())
    }
}

pub async fn load_module(
    stream_dir: &Path,
    module_cid: Cid,
) -> anyhow::Result<(Arc<dyn LeafModule>, libsql::Connection)> {
    let Some(module) = STORAGE.get_module(module_cid).await? else {
        anyhow::bail!("Could not load module needed by stream: {module_cid}");
    };
    let module_db_name = &format!("module_{module_cid}.db");
    let module_db_path = stream_dir.join(module_db_name);
    let module_db = libsql::Builder::new_local(module_db_path)
        .build()
        .await
        .context("error opening module db")?
        .connect()?;

    // List the files in the stream directory
    let mut read_dir = tokio::fs::read_dir(stream_dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let filename = entry.file_name();
        let filename = filename.as_os_str().to_string_lossy();

        // Clean up ( remove ) any module database files that are not for the module that we just
        // loaded.
        if filename.starts_with("module_") && !filename.starts_with(module_db_name) {
            tokio::fs::remove_file(entry.path()).await?;
        }
    }

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
