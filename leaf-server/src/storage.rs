use std::{
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
    time::Duration,
};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use blake3::Hash;
use leaf_stream::{StreamGenesis, modules::wasm::LeafWasmModule, validate_wasm};
use leaf_utils::convert::{FromRows, ParseRow};
use libsql::Connection;
use parity_scale_codec::Decode;
use tracing::{Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use weak_table::WeakValueHashMap;

use crate::{
    async_oncelock::AsyncOnceLock,
    streams::{STREAMS, StreamHandle},
};

pub static STORAGE: LazyLock<Storage> = LazyLock::new(Storage::default);

static WASM_MODULES: LazyLock<RwLock<WeakValueHashMap<Hash, Weak<LeafWasmModule>>>> =
    LazyLock::new(|| RwLock::new(WeakValueHashMap::new()));

#[derive(Default)]
pub struct Storage {
    db: AsyncOnceLock<libsql::Connection>,
    data_dir: RwLock<Option<PathBuf>>,
}

impl Storage {
    async fn db(&self) -> &libsql::Connection {
        (&self.db).await
    }

    pub fn data_dir(&self) -> anyhow::Result<PathBuf> {
        if let Some(lock) = &self.data_dir.try_read()
            && let Some(d) = lock.as_ref()
        {
            Ok(d.clone())
        } else {
            anyhow::bail!("Storage not initialized");
        }
    }

    #[instrument(skip(self), err)]
    pub async fn initialize(&self, data_dir: &Path) -> anyhow::Result<()> {
        if self.db.has_initialized() {
            tracing::warn!("Database already initialized.");
            return Ok(());
        }

        // Open database file
        *self.data_dir.write().await = Some(data_dir.to_owned());
        tokio::fs::create_dir_all(data_dir).await?;
        let database = libsql::Builder::new_local(data_dir.join("leaf.db"))
            .build()
            .await?;
        let c = database.connect()?;
        c.execute_batch("pragma synchronous = normal; pragma journal_mode = wal;").await?;
        tracing::info!("database connected");

        // Run migrations
        run_database_migrations(&c).await?;

        // Start the background storage tasks
        start_background_tasks();

        self.db.set(c).ok();

        Ok(())
    }

    /// Returns the list of hashes that could be found in the database out of the list that is
    /// provided to search for.
    #[instrument(skip(self, hash), err)]
    pub async fn has_blob(&self, hash: Hash) -> anyhow::Result<bool> {
        let mut rows = self
            .db()
            .await
            .query(
                "select hash from wasm_blobs where hash = ?",
                [hash.as_bytes().to_vec()],
            )
            .await?;
        return Ok(rows.next().await?.is_some());
    }

    #[instrument(skip(self), err)]
    pub async fn get_wasm_blob(&self, id: Hash) -> anyhow::Result<Option<Vec<u8>>> {
        let mut blobs = Vec::<Vec<u8>>::from_rows(
            self.db()
                .await
                .query(
                    "select data from wasm_blobs where hash=?",
                    [id.as_bytes().to_vec()],
                )
                .await?,
        )
        .await?;
        Ok(blobs.pop())
    }

    #[instrument(skip(self), err)]
    pub async fn get_wasm_module(&self, id: Hash) -> anyhow::Result<Option<Arc<LeafWasmModule>>> {
        let modules = WASM_MODULES.upgradable_read().await;
        if let Some(module) = modules.get(&id) {
            return Ok(Some(module));
        }
        let Some(blob) = self.get_wasm_blob(id).await? else {
            return Ok(None);
        };
        let module = Arc::new(LeafWasmModule::new(&blob)?);
        let mut modules = RwLockUpgradableReadGuard::upgrade(modules).await;
        modules.insert(id, module.clone());

        Ok(Some(module))
    }

    #[instrument(skip(self), err)]
    pub async fn create_stream(&self, genesis: StreamGenesis) -> anyhow::Result<StreamHandle> {
        let (id, genesis_bytes) = genesis.get_stream_id_and_bytes();
        let creator = genesis.creator.clone();
        let genesis_module = genesis.module;

        let stream = STREAMS.load(genesis).await?;

        self.db()
            .await
            .execute(
                "insert into streams (id, creator, genesis, current_module) \
                values (:id, :creator, :genesis, :current_module)",
                (
                    (":id", id.as_bytes().to_vec()),
                    (":creator", creator),
                    (":genesis", genesis_bytes),
                    (":current_module", genesis_module.0.as_bytes().to_vec()),
                ),
            )
            .await?;

        Ok(stream)
    }

    /// Update the recorded current module for the stream in the leaf database.
    ///
    /// This should be called after a module change has been made to the stream so that the database
    /// can avoid garbage collecting the new WASM module that it depends on.
    #[instrument(skip(self), err)]
    pub async fn update_stream_current_module(
        &self,
        stream: Hash,
        current_module: Hash,
    ) -> anyhow::Result<()> {
        self.db()
            .await
            .execute(
                "update streams set current_module = :module where id = :id",
                (
                    (":module", current_module.as_bytes().to_vec()),
                    (":id", stream.as_bytes().to_vec()),
                ),
            )
            .await?;

        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn open_stream(&self, id: Hash) -> anyhow::Result<Option<StreamHandle>> {
        let mut rows = self
            .db()
            .await
            .query(
                "select genesis from streams where id = ?",
                [id.as_bytes().to_vec()],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let genesis_bytes: Vec<u8> = row.parse_row().await?;
        let genesis = StreamGenesis::decode(&mut &genesis_bytes[..])?;

        Ok(Some(STREAMS.load(genesis).await?))
    }

    #[instrument(skip(self, data), err)]
    pub async fn upload_wasm(&self, creator: &str, data: Vec<u8>) -> anyhow::Result<blake3::Hash> {
        if data.len() > 1024 * 1024 * 10 {
            anyhow::bail!("WASM module larger than 10MB maximum size.");
        }
        validate_wasm(&data)?;
        let trans = self.db().await.transaction().await?;
        let hash = blake3::hash(&data);
        trans
            .execute(
                r#"insert or ignore into wasm_blobs (hash, data) values (:hash, :data)"#,
                ((":hash", hash.as_bytes().to_vec()), (":data", data)),
            )
            .await?;
        trans
            .execute(
                r#"insert into staged_wasm (creator, hash) values (:creator, :hash)"#,
                ((":creator", creator), (":hash", hash.as_bytes().to_vec())),
            )
            .await?;
        trans.commit().await?;
        Ok(hash)
    }

    #[instrument(skip(self), err)]
    pub async fn garbage_collect_wasm(&self) -> anyhow::Result<()> {
        let mut deleted = self
            .db()
            .await
            .execute_batch(
                r#"
                delete from staged_wasm where (unixepoch() - timestamp) > 500 returning creator, hash;
                delete from wasm_blobs where not exists (
                    select 1 from staged_wasm s where s.hash=wasm_blobs.hash
                        union
                    select 1 from streams s where s.current_module=wasm_blobs.hash
                ) returning hash;
                "#,
            )
            .await?;
        let mut deleted_staged: Vec<(String, Hash)> = Vec::new();
        let mut deleted_blobs: Vec<Hash> = Vec::new();
        let r = async {
            if let Some(rows) = deleted.next_stmt_row().flatten() {
                deleted_staged = Vec::from_rows(rows).await?;
            }
            if let Some(rows) = deleted.next_stmt_row().flatten() {
                deleted_blobs = Vec::from_rows(rows).await?;
            }
            anyhow::Ok(())
        }
        .await;
        if let Err(error) = r {
            tracing::warn!(%error, "Error parsing deleted WASM blobs")
        }

        if !deleted_staged.is_empty() || !deleted_blobs.is_empty() {
            tracing::info!(
                deleted_staged_wasm_modules=?deleted_staged,
                deleted_wasm_blobs=?deleted_blobs,
                "Garbage collected WASM records"
            );
        }
        let span = Span::current();
        span.set_attribute("wasm_blobs_deleted", deleted_staged.len() as i64);
        span.set_attribute("staged_wasm_modules_deleted", deleted_blobs.len() as i64);

        Ok(())
    }
}

#[instrument(skip(db))]
pub async fn run_database_migrations(db: &Connection) -> anyhow::Result<()> {
    db.execute_transactional_batch(include_str!("schema/schema_00.sql"))
        .await?;
    Ok(())
}

pub fn start_background_tasks() {
    // Garbage collect WASM files periodically
    tokio::spawn(async move {
        loop {
            STORAGE.garbage_collect_wasm().await.ok();
            tokio::time::sleep(Duration::from_secs(500)).await;
        }
    });
}
