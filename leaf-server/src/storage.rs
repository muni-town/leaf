use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
    time::Duration,
};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use blake3::Hash;
use leaf_stream::{StreamGenesis, modules::wasm::LeafWasmModule, validate_wasm};
use leaf_utils::convert::{FromRows, ParseRow, ParseRows};
use libsql::Connection;
use miniz_oxide::{deflate::compress_to_vec, inflate::decompress_to_vec};
use parity_scale_codec::Decode;
use reqwest::Url;
use s3_simple::Bucket;
use tracing::{Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use weak_table::WeakValueHashMap;

use crate::{
    async_oncelock::AsyncOnceLock,
    streams::{STREAMS, StreamHandle},
};

pub static GLOBAL_SQLITE_PRAGMA: &str = "pragma synchronous = normal; pragma journal_mode = wal;";

pub static STORAGE: LazyLock<Storage> = LazyLock::new(Storage::default);

static WASM_MODULES: LazyLock<RwLock<WeakValueHashMap<Hash, Weak<LeafWasmModule>>>> =
    LazyLock::new(|| RwLock::new(WeakValueHashMap::new()));

pub static BUCKET_MODULE_DIR: &str = "modules";

#[derive(Default)]
pub struct Storage {
    db: AsyncOnceLock<libsql::Connection>,
    data_dir: RwLock<Option<PathBuf>>,
    backup_bucket: RwLock<Option<Bucket>>,
}

#[derive(Debug, Clone, clap::Args)]
#[group(required = false, multiple = true)]
pub struct S3BackupConfig {
    #[arg(long = "s3-host", env = "S3_HOST")]
    pub host: Url,
    #[arg(long = "s3-bucket", env = "S3_BUCKET")]
    pub name: String,
    #[arg(long = "s3-region", env = "S3_REGION")]
    pub region: String,
    #[arg(long = "s3-access-key", env = "S3_ACCESS_KEY")]
    pub access_key: String,
    #[arg(long = "s3-secret-key", env = "S3_SECRET_KEY")]
    pub secret_key: String,
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
    pub async fn initialize(
        &self,
        data_dir: &Path,
        s3_backup: Option<S3BackupConfig>,
    ) -> anyhow::Result<()> {
        if self.db.has_initialized() {
            tracing::warn!("Database already initialized.");
            return Ok(());
        }

        // Setup the S3 bucket client if configured
        *self.backup_bucket.write().await = s3_backup
            .map(|config| {
                Bucket::new(
                    config.host,
                    config.name,
                    s3_simple::Region(config.region),
                    s3_simple::Credentials {
                        access_key_id: s3_simple::AccessKeyId(config.access_key),
                        access_key_secret: s3_simple::AccessKeySecret(config.secret_key),
                    },
                    None,
                )
            })
            .transpose()?;

        // Open database file
        *self.data_dir.write().await = Some(data_dir.to_owned());
        tokio::fs::create_dir_all(data_dir).await?;
        let database = libsql::Builder::new_local(data_dir.join("leaf.db"))
            .build()
            .await?;
        let c = database.connect()?;
        c.execute_batch(GLOBAL_SQLITE_PRAGMA).await?;
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

    /// List the WASM module blobs
    pub async fn list_wasm_modules(&self) -> anyhow::Result<HashSet<Hash>> {
        let modules: Vec<Hash> = self
            .db()
            .await
            .query("select hash from wasm_blobs", ())
            .await?
            .parse_rows()
            .await?;
        Ok(modules.into_iter().collect())
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

    /// Persist the latest event we have for a stream.
    pub async fn set_latest_event(&self, stream_id: Hash, latest_event: i64) -> anyhow::Result<()> {
        self.db()
            .await
            .execute(
                "update streams set latest_event = ? where id = ?",
                (latest_event, stream_id.as_bytes().to_vec()),
            )
            .await?;
        Ok(())
    }

    /// Get the list of streams and the latest event we have for each
    pub async fn get_latest_events(&self) -> anyhow::Result<Vec<(Hash, Option<i64>)>> {
        let rows: Vec<(Hash, Option<i64>)> = self
            .db()
            .await
            .query("select id, latest_event from streams", ())
            .await?
            .parse_rows()
            .await?;
        Ok(rows)
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

    /// Add a WASM blob directly.
    ///
    /// This is used internally by the s3 backup script.
    #[instrument(skip(self, data), err)]
    async fn add_wasm_blob(&self, data: Vec<u8>) -> anyhow::Result<()> {
        let hash = blake3::hash(&data);
        self.db()
            .await
            .execute(
                r#"insert or ignore into wasm_blobs (hash, data) values (:hash, :data)"#,
                ((":hash", hash.as_bytes().to_vec()), (":data", data)),
            )
            .await?;
        Ok(())
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

        // Delete all the deleted blobs from the S3 backup
        if let Some(bucket) = &*self.backup_bucket.read().await {
            for deleted in &deleted_blobs {
                bucket
                    .delete(format!("{BUCKET_MODULE_DIR}/{deleted}"))
                    .await
                    .ok();
            }
        }

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

    #[instrument(skip(self), err)]
    pub async fn backup_to_s3(&self) -> anyhow::Result<()> {
        let Some(bucket) = &*self.backup_bucket.read().await else {
            return Ok(());
        };

        // List wasm modules in the bucket
        let modules_in_backup = bucket
            .list(BUCKET_MODULE_DIR, None)
            .await?
            .into_iter()
            .filter_map(|x| Hash::from_hex(x.name.strip_suffix(".wasm").unwrap_or_default()).ok())
            .collect::<HashSet<_>>();
        // List wasm modules we have locally
        let modules_local = self.list_wasm_modules().await?;

        // Upload local modules not in backup
        for module_not_in_backup in modules_local.difference(&modules_in_backup) {
            let Some(module) = self.get_wasm_blob(*module_not_in_backup).await? else {
                continue;
            };
            bucket
                .put(
                    format!("{BUCKET_MODULE_DIR}/{module_not_in_backup}.wasm.gz"),
                    &compress_to_vec(&module, 6),
                )
                .await?;
        }

        // Download backup modules that aren't local
        for module_not_local in modules_local.difference(&modules_in_backup) {
            let resp = bucket
                .get(format!("{BUCKET_MODULE_DIR}/{module_not_local}.wasm.gz"))
                .await?;
            let data = decompress_to_vec(&resp.bytes().await?).map_err(|e| {
                anyhow::format_err!("Could not decompress WASM module from s3 backup: {e}")
            })?;
            self.add_wasm_blob(data).await?;
        }

        let latest_events = self.get_latest_events().await?;
        for (hash, latest_event) in latest_events {}

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

    // Backup streams to s3
    tokio::spawn(async move {
        loop {
            STORAGE.backup_to_s3().await.ok();
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    });
}
