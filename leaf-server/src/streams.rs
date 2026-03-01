use std::{
    path::Path,
    sync::{Arc, LazyLock, Weak},
};

use anyhow::Context;
use leaf_stream::{LeafModule, Stream, atproto_plc::Did, dasl::cid::Cid};
use tokio::sync::RwLock;
use weak_table::WeakValueHashMap;

use crate::storage::{GLOBAL_SQLITE_PRAGMA, STORAGE};
use crate::unreads::UnreadsDB;

/// Global cache of open Leaf streams with their unreads database connections.
pub static STREAMS: LazyLock<Streams> = LazyLock::new(Streams::default);

/// A stream handle with its associated unreads database connection.
pub struct StreamWithUnreads {
    pub stream: Arc<Stream>,
    pub unreads_db: UnreadsDB,
}

pub type StreamHandle = Arc<StreamWithUnreads>;

#[derive(Default)]
pub struct Streams {
    streams: RwLock<WeakValueHashMap<Did, Weak<StreamWithUnreads>>>,
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
        let latest_event_rx = stream.subscribe_updates().await;
        let id_ = id.clone();
        tokio::spawn(async move {
            while let Ok(update) = latest_event_rx.recv().await {
                if let Err(e) = STORAGE.set_stream_updated(id_.clone(), update).await {
                    tracing::error!("Error updating latest event for stream in database: {e}");
                }
            }
        });

        // Initialize the unreads database for this stream
        let unreads_db = UnreadsDB::initialize(&stream_dir).await?;

        // Store the stream handle in the cache
        let stream = Arc::new(stream);
        let handle = Arc::new(StreamWithUnreads { stream, unreads_db });
        self.streams
            .write()
            .await
            .insert(id.clone(), handle.clone());

        // Start monitoring for unreads tracking
        if let Err(e) = crate::unreads_materializer::UNREADS_MATERIALIZER
            .start_monitoring(id.clone(), handle.clone())
            .await
        {
            tracing::warn!("Failed to start unreads monitoring for stream {id}: {e}");
        }

        // Return the stream handle
        Ok(handle)
    }

    pub async fn update_module(
        &self,
        s: Arc<StreamWithUnreads>,
        module_cid: Cid,
    ) -> anyhow::Result<()> {
        let data_dir = STORAGE.data_dir()?;
        let stream_dir = data_dir.join("streams").join(s.stream.id().as_str());

        s.stream.raw_set_module(Some(module_cid)).await?;
        let (module, db) = load_module(&stream_dir, module_cid).await?;
        s.stream.provide_module(module, db).await?;
        STORAGE
            .update_stream_module(s.stream.id().clone(), module_cid)
            .await?;

        Ok(())
    }

    /// Stop monitoring a stream for unreads tracking.
    /// This is called when a stream is dropped from the cache.
    #[tracing::instrument(skip(self))]
    pub async fn stop_monitoring(&self, id: &Did) -> anyhow::Result<()> {
        // Stop monitoring via the materializer
        if let Err(e) = crate::unreads_materializer::UNREADS_MATERIALIZER
            .stop_monitoring(id)
            .await
        {
            tracing::warn!("Failed to stop unreads monitoring for stream {id}: {e}");
        }

        Ok(())
    }

    /// Cleanup monitoring tasks for streams that are no longer in the cache.
    /// This should be called periodically to prevent memory leaks from orphaned monitoring tasks.
    #[tracing::instrument(skip(self))]
    pub async fn cleanup_monitoring(&self) -> anyhow::Result<()> {
        // Get all stream DIDs currently in the cache
        let cached_streams: Vec<Did> = {
            let streams = self.streams.read().await;
            streams.keys().cloned().collect()
        };

        // Get all stream DIDs currently being monitored
        let monitored_streams = crate::unreads_materializer::UNREADS_MATERIALIZER
            .get_monitored_streams()
            .await;

        // Stop monitoring for streams that are not in the cache
        for stream_did in monitored_streams {
            if !cached_streams.contains(&stream_did) {
                tracing::info!("Cleaning up orphaned monitoring task for stream {stream_did}");
                if let Err(e) = self.stop_monitoring(&stream_did).await {
                    tracing::warn!("Failed to stop monitoring for stream {stream_did}: {e}");
                }
            }
        }

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

    // Attach the state DB to the module DB
    // SQLite will create the file automatically if it doesn't exist
    module_db
        .execute(
            "attach ? as state",
            [stream_dir.join("state.db").to_string_lossy().to_string()],
        )
        .await?;

    Ok((module, module_db))
}
