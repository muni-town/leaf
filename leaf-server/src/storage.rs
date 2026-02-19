use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, LazyLock, Weak},
    time::Duration,
};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use leaf_stream::{
    BasicModule, LeafModule, StreamUpdate,
    atproto_plc::{Did, SigningKey},
    dasl::{
        self,
        cid::{Cid, Codec::Drisl},
    },
    types::{Event, ModuleCodec},
};
use leaf_utils::convert::{FromRows, ParseRows};
use libsql::Connection;
use reqwest::Url;
use s3_simple::Bucket;
use serde::{Deserialize, Serialize};
use tracing::{Span, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use weak_table::WeakValueHashMap;
use zeroize::ZeroizeOnDrop;

use crate::{
    async_oncelock::AsyncOnceLock,
    streams::{STREAMS, StreamHandle},
};

pub static GLOBAL_SQLITE_PRAGMA: &str = "pragma synchronous = normal; pragma journal_mode = wal;";

pub static STORAGE: LazyLock<Storage> = LazyLock::new(Storage::default);

static MODULES: LazyLock<RwLock<WeakValueHashMap<Cid, Weak<dyn LeafModule>>>> =
    LazyLock::new(|| RwLock::new(WeakValueHashMap::new()));

pub const BUCKET_MODULE_DIR: &str = "modules";
pub const BUCKET_STREAMS_DIR: &str = "streams";
pub const MODULE_ARCHIVE_EXT: &str = ".module.drisl.zstd";
pub const EVENTS_ARCHIVE_EXT: &str = ".events.drisl.zstd";
pub const STATEDB_FILENAME: &str = "state.db.zstd";
pub const METADATA_FILENAME: &str = "metadata.drisl";

#[derive(Default)]
pub struct Storage {
    db: AsyncOnceLock<libsql::Connection>,
    data_dir: RwLock<Option<PathBuf>>,
    backup_bucket: RwLock<Option<Bucket>>,
}

#[derive(Debug, Clone, clap::Args)]
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

#[derive(Debug, Clone)]
pub struct ListStreamsItem {
    pub did: Did,
    pub owners: Vec<String>,
    pub module_cid: Option<Cid>,
    pub latest_event: Option<i64>,
    pub state_db_updated_at: Option<i64>,
    pub state_db_backed_up_at: Option<i64>,
    pub backup_latest_event: Option<i64>,
    pub backup_module_cid: Option<Cid>,
    pub backup_owners: Option<Vec<String>>,
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

    /// Initialize the storage singleton
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
                    Some(s3_simple::BucketOptions {
                        path_style: true,
                        list_objects_v2: false,
                    }),
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
    #[instrument(skip(self, cid), err)]
    pub async fn has_module_blob(&self, cid: Cid) -> anyhow::Result<bool> {
        let mut rows = self
            .db()
            .await
            .query(
                "select 1 from module_blobs where cid = ?",
                [cid.as_bytes().to_vec()],
            )
            .await?;
        return Ok(rows.next().await?.is_some());
    }

    // Get a moudle blob from the database
    #[instrument(skip(self), err)]
    pub async fn get_module_blob(&self, cid: Cid) -> anyhow::Result<Option<Vec<u8>>> {
        let mut blobs = Vec::<Vec<u8>>::from_rows(
            self.db()
                .await
                .query(
                    "select data from module_blobs where cid=?",
                    [cid.as_bytes().to_vec()],
                )
                .await?,
        )
        .await?;
        Ok(blobs.pop())
    }

    /// List the module blobs in the database
    pub async fn list_module_blobs(&self) -> anyhow::Result<HashSet<Cid>> {
        let modules: Vec<Cid> = self
            .db()
            .await
            .query("select cid from module_blobs", ())
            .await?
            .parse_rows()
            .await?;
        Ok(modules.into_iter().collect())
    }

    /// Get a module from the database
    pub async fn get_module(&self, module_cid: Cid) -> anyhow::Result<Option<Arc<dyn LeafModule>>> {
        let modules = MODULES.upgradable_read().await;
        if let Some(module) = modules.get(&module_cid) {
            return Ok(Some(module));
        }

        let blob = self
            .get_module_blob(module_cid)
            .await?
            .ok_or_else(|| anyhow::format_err!("module blob not found"))?;
        let codec = ModuleCodec::decode(&blob[..])?;

        let module = match codec.module_type() {
            x if x == BasicModule::module_type_id() => {
                Arc::new(BasicModule::load(codec)?) as Arc<dyn LeafModule>
            }
            _ => anyhow::bail!("cannot load module: unrecognized module type ID"),
        };

        let mut modules = RwLockUpgradableReadGuard::upgrade(modules).await;
        modules.insert(module_cid, module.clone());

        Ok(Some(module))
    }

    /// Create a new stream
    #[instrument(skip(self), err)]
    pub async fn create_stream(
        &self,
        stream_did: Did,
        owner: String,
    ) -> anyhow::Result<StreamHandle> {
        let stream = STREAMS.load(stream_did.clone()).await?;

        self.db()
            .await
            .execute(
                "insert into streams (did, module_cid, latest_event) values (?, null, 0)",
                [stream_did.as_str().to_string()],
            )
            .await?;

        Ok(stream)
    }

    /// Set the updated time for a stream and optionally update the latest event too.
    ///
    /// There may not be an update for the latest event if the change to the stream was to the state
    /// DB from ephemeral state events.
    pub async fn set_stream_updated(
        &self,
        stream_did: Did,
        stream_update: StreamUpdate,
    ) -> anyhow::Result<()> {
        let db = self.db().await;

        match stream_update {
            StreamUpdate::NewEvents { latest_idx } => {
                db.execute(
                    "update streams set latest_event = ? where did = ?",
                    (latest_idx, stream_did.as_str().to_string()),
                )
                .await?;
            }
            StreamUpdate::StateChanged => {
                db.execute(
                    "
                        insert into backup_status (did) values (?) \
                        on conflict do nothing
                    ",
                    [stream_did.as_str().to_string()],
                )
                .await?;
                db.execute(
                    "
                        update backup_status
                        set state_db_updated_at = unixepoch()
                        where did = ?
                    ",
                    [stream_did.as_str().to_string()],
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Set the backed up time for a stream.
    async fn set_stream_backed_up(
        &self,
        stream_did: Did,
        update: BackupUpdate,
    ) -> anyhow::Result<()> {
        let db = self.db().await;
        db.execute(
            "
                    insert into backup_status (did) values (?) \
                    on conflict do nothing
                ",
            [stream_did.as_str().to_string()],
        )
        .await?;

        match update {
            BackupUpdate::Stream(stream_update) => match stream_update {
                StreamUpdate::NewEvents { latest_idx } => {
                    db.execute(
                        "
                        update backup_status
                        set backup_latest_event = ?
                        where did = ?
                    ",
                        (latest_idx, stream_did.as_str().to_string()),
                    )
                    .await?;
                }
                StreamUpdate::StateChanged => {
                    db.execute(
                        "
                        update backup_status
                        set state_db_backed_up_at = unixepoch()
                        where did = ?
                    ",
                        [stream_did.as_str().to_string()],
                    )
                    .await?;
                }
            },
            BackupUpdate::Metadata { owners, module_cid } => {
                db.execute(
                    "
                        update backup_status
                        set backup_module_cid = ?, backup_owners = ?
                        where did = ?
                    ",
                    (
                        module_cid.map(|x| x.to_string()),
                        serde_json::to_string(&owners).unwrap(),
                        stream_did.as_str().to_string(),
                    ),
                )
                .await?;
            }
        }

        Ok(())
    }

    pub async fn reset_backup_cache(&self) -> anyhow::Result<()> {
        let db = self.db().await;
        db.execute("delete from backup_status", ()).await?;
        Ok(())
    }

    /// Get the list of streams and the latest event we have for each
    pub async fn list_streams(&self) -> anyhow::Result<Vec<ListStreamsItem>> {
        #[allow(clippy::type_complexity)]
        let rows: Vec<(
            Did,
            Option<Cid>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<Cid>,
            Option<String>,
        )> = self
            .db()
            .await
            .query(
                "
                select
                    streams.did, 
                    module_cid, 
                    latest_event,
                    state_db_updated_at,
                    state_db_backed_up_at,
                    backup_latest_event,
                    backup_module_cid,
                    backup_owners \
                from streams \
                left join backup_status \
                    on streams.did = backup_status.did",
                (),
            )
            .await?
            .parse_rows()
            .await?;
        let mut items = rows
            .into_iter()
            .map(
                |(
                    did,
                    module_cid,
                    latest_event,
                    state_db_updated_at,
                    state_db_backed_up_at,
                    backup_latest_event,
                    backup_module_cid,
                    backup_owners,
                )| {
                    Ok::<_, anyhow::Error>(ListStreamsItem {
                        did,
                        owners: Vec::new(), // Note: we will fetch the owners below
                        module_cid,
                        latest_event,
                        state_db_updated_at,
                        state_db_backed_up_at,
                        backup_latest_event,
                        backup_module_cid,
                        backup_owners: backup_owners
                            .map(|x| serde_json::from_str::<Vec<String>>(&x))
                            .transpose()?,
                    })
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        // Fetch the owners for each stream
        for item in items.iter_mut() {
            item.owners = self.get_did_owners(item.did.clone()).await?;
        }

        Ok(items)
    }

    /// Update the recorded current module for the stream in the leaf database.
    ///
    /// This should be called after a module change has been made to the stream so that the database
    /// can avoid garbage collecting the new module that it depends on.
    #[instrument(skip(self), err)]
    pub async fn update_stream_module(
        &self,
        stream_did: Did,
        module_cid: Cid,
    ) -> anyhow::Result<()> {
        self.db()
            .await
            .execute(
                "update streams set module_cid = ? where did = ?",
                (
                    module_cid.as_bytes().to_vec(),
                    stream_did.as_str().to_string(),
                ),
            )
            .await?;

        Ok(())
    }

    /// Add a module blob directly.
    ///
    /// This is used internally by the s3 backup script.
    #[instrument(skip(self, data), err)]
    async fn add_module_blob(&self, data: Vec<u8>) -> anyhow::Result<Cid> {
        let cid = Cid::digest_sha2(Drisl, &data);
        self.db()
            .await
            .execute(
                r#"insert or ignore into module_blobs (cid, data) values (?, ?)"#,
                (cid.as_bytes().to_vec(), data),
            )
            .await?;
        Ok(cid)
    }

    #[instrument(skip(self), err)]
    pub async fn upload_module(&self, creator: &str, module: ModuleCodec) -> anyhow::Result<Cid> {
        let trans = self.db().await.transaction().await?;
        let (cid, data) = module.encode();
        trans
            .execute(
                r#"insert or ignore into module_blobs (cid, data) values (?, ?)"#,
                (cid.as_bytes().to_vec(), data),
            )
            .await?;
        trans
            .execute(
                r#"insert into staged_modules (creator, cid) values (?, ?)"#,
                (creator, cid.as_bytes().to_vec()),
            )
            .await?;
        trans.commit().await?;
        Ok(cid)
    }

    #[instrument(skip(self), err)]
    pub async fn garbage_collect_modules(&self) -> anyhow::Result<()> {
        let mut deleted = self
            .db()
            .await
            .execute_batch(
                r#"
                delete from staged_modules where (unixepoch() - timestamp) > 500 returning creator, cid;
                delete from module_blobs where not exists (
                    select 1 from staged_modules s where s.cid=module_blobs.cid
                        union
                    select 1 from streams s where s.module_cid=module_blobs.cid
                ) returning cid;
                "#,
            )
            .await?;
        let mut deleted_staged: Vec<(String, Cid)> = Vec::new();
        let mut deleted_blobs: Vec<Cid> = Vec::new();
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
            tracing::warn!(%error, "Error parsing deleted module blobs")
        }

        if !deleted_staged.is_empty() || !deleted_blobs.is_empty() {
            tracing::info!(
                deleted_staged_modules=?deleted_staged,
                deleted_module_blobs=?deleted_blobs,
                "Garbage collected modules"
            );
        }
        let span = Span::current();
        span.set_attribute("module_blobs_deleted", deleted_staged.len() as i64);
        span.set_attribute("staged_modules_deleted", deleted_blobs.len() as i64);

        Ok(())
    }

    #[instrument(skip(self, key), err)]
    pub async fn create_did(&self, did: Did, key: SigningKey, owner: String) -> anyhow::Result<()> {
        let db = self.db().await;
        db.execute("begin immediate", ()).await?;
        db.execute(
            "insert into dids (did) values (?)",
            [did.as_str().to_string()],
        )
        .await?;
        db.execute(
            "insert into did_keys (did, p256_key, k256_key) values (?, ?, ?)",
            (
                did.as_str().to_string(),
                if let SigningKey::P256(key) = &key {
                    Some(key.to_bytes().to_vec())
                } else {
                    None
                },
                if let SigningKey::K256(key) = &key {
                    Some(key.to_bytes().to_vec())
                } else {
                    None
                },
            ),
        )
        .await?;
        db.execute(
            "insert into did_owners (did, owner) values (?, ?)",
            [did.as_str().to_string(), owner],
        )
        .await?;
        db.execute("commit", ()).await?;
        Ok(())
    }

    #[instrument(skip(self, did), err)]
    pub async fn get_did_signing_key(&self, did: Did) -> anyhow::Result<SigningKey> {
        let db = self.db().await;

        #[allow(clippy::type_complexity)]
        let keys: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> = db
            .query(
                "select p256_key, k256_key from did_keys where did = ?",
                [did.as_str().to_string()],
            )
            .await?
            .parse_rows()
            .await?;
        let mut keys = keys.into_iter().filter_map(|(p256, k256)| {
            if let Some(bytes) = p256 {
                Some(SigningKey::P256(
                    p256::ecdsa::SigningKey::from_slice(&bytes).ok()?,
                ))
            } else if let Some(bytes) = k256 {
                Some(SigningKey::K256(
                    k256::ecdsa::SigningKey::from_slice(&bytes).ok()?,
                ))
            } else {
                None
            }
        });
        keys.next()
            .ok_or_else(|| anyhow::format_err!("No signing key found for DID"))
    }

    #[instrument(skip(self), err)]
    pub async fn get_did_owners(&self, did: Did) -> anyhow::Result<Vec<String>> {
        let owners: Vec<String> = self
            .db()
            .await
            .query(
                "select owner from did_owners where did = ?",
                [did.as_str().to_string()],
            )
            .await?
            .parse_rows()
            .await?;
        Ok(owners)
    }

    #[instrument(skip(self), err)]
    pub async fn backup_to_s3(&self) -> anyhow::Result<()> {
        let Some(bucket) = &*self.backup_bucket.read().await else {
            return Ok(());
        };

        // List modules in the bucket
        let modules_in_backup = bucket
            .list(BUCKET_MODULE_DIR, None)
            .await?
            .into_iter()
            .flat_map(|x| x.contents.into_iter())
            .filter_map(|x| {
                Cid::from_str(
                    x.key
                        .strip_prefix(&format!("{BUCKET_MODULE_DIR}/"))
                        .unwrap_or_default()
                        .strip_suffix(MODULE_ARCHIVE_EXT)
                        .unwrap_or_default(),
                )
                .ok()
            })
            .collect::<HashSet<_>>();

        // List modules we have locally
        let modules_local = self.list_module_blobs().await?;

        // Upload local modules not in backup
        for module_not_in_backup in modules_local.difference(&modules_in_backup) {
            let Some(module) = self.get_module_blob(*module_not_in_backup).await? else {
                continue;
            };
            let compressed = zstd::encode_all(&module[..], 0)?;
            bucket
                .put(
                    format!("{BUCKET_MODULE_DIR}/{module_not_in_backup}{MODULE_ARCHIVE_EXT}"),
                    &compressed,
                )
                .await?;
        }

        // Get the list of streams we have on the server
        let streams_local = self.list_streams().await?;

        // For each stream we have locally
        for stream in streams_local {
            // Check whether the stream events need to be backed up
            let stream_events_need_backup =
                stream.backup_latest_event.unwrap_or(0) < stream.latest_event.unwrap_or(0);

            // Check whether the state DB needs to be backed up
            let state_db_needs_backup =
                stream.state_db_backed_up_at.unwrap_or(0) < stream.state_db_updated_at.unwrap_or(0);

            // Check whether the module ID changed
            let module_cid_changed = stream.module_cid != stream.backup_module_cid;

            // Check whether or not the owners have changed
            let owners_changed = Some(&stream.owners) != stream.backup_owners.as_ref();

            // Check whether the stream metadata needs to be backed up
            let metadata_needs_backup = module_cid_changed || owners_changed;

            let needs_any_backup =
                metadata_needs_backup || state_db_needs_backup || stream_events_need_backup;

            // Skip if there is nothing to backup
            if !needs_any_backup {
                continue;
            }

            // Backup the stream's metadata if needed
            if metadata_needs_backup {
                let metadata = StreamMetadata {
                    did: stream.did.clone(),
                    did_key: self.get_did_signing_key(stream.did.clone()).await?.into(),
                    owners: stream.owners.clone(),
                    module_cid: stream.module_cid,
                };
                let metadata_bytes = dasl::drisl::to_vec(&metadata)?;
                let stream_did = &stream.did;
                let file_name = format!("{BUCKET_STREAMS_DIR}/{stream_did}/{METADATA_FILENAME}");
                bucket.put(file_name, &metadata_bytes).await?;

                // Record the backup status for the metadata
                self.set_stream_backed_up(
                    stream.did.clone(),
                    BackupUpdate::Metadata {
                        owners: stream.owners.clone(),
                        module_cid: stream.module_cid,
                    },
                )
                .await?;
            }

            // Backup the stream's events if needed
            'events_backup: {
                if stream_events_need_backup {
                    // Open the local stream
                    let s = STREAMS.load(stream.did.clone()).await?;

                    // Get the starting event in the range we need to backup
                    let start_event = stream.backup_latest_event.map(|l| l + 1).unwrap_or(1);

                    // Get the events that are newer than the latest backed up event.
                    let events = s
                        .raw_get_events((stream.backup_latest_event.map(|l| l + 1).unwrap_or(1))..)
                        .await?;
                    let events_len = events.len() as i64;

                    // Skip stream backup if there are no events in the range
                    if events_len == 0 {
                        break 'events_backup;
                    }

                    // Get the end idx in the range we are backing up
                    let end_event = start_event + events_len - 1;

                    // Create and compress the event archive
                    let archive = EventArchive { events };
                    let archive_bytes = dasl::drisl::to_vec(&archive)?;
                    let compressed = zstd::encode_all(&archive_bytes[..], 0)?;

                    // Push the archive to s3
                    let stream_did = &stream.did;
                    let name = format!(
                        "{BUCKET_STREAMS_DIR}/{stream_did}/{start_event}-{end_event}{EVENTS_ARCHIVE_EXT}"
                    );
                    bucket.put(name, &compressed).await?;

                    // Record the backup status for the stream events
                    self.set_stream_backed_up(
                        stream.did.clone(),
                        BackupUpdate::Stream(StreamUpdate::NewEvents {
                            latest_idx: end_event,
                        }),
                    )
                    .await?;
                }
            }

            // Backup the stream's state DB if needed
            // TODO: this needs more testing to make sure it works, once we start using it.
            if state_db_needs_backup {
                let temp_dir = tempfile::tempdir()?;
                let statedb_path = self
                    .data_dir()?
                    .join("streams")
                    .join(stream.did.as_str())
                    .join("state.db");
                let statedb_backup_path = temp_dir.path().join("state.db");

                // Open a new connection to the state database
                let statedb = libsql::Builder::new_local(statedb_path)
                    .build()
                    .await?
                    .connect()?;

                // Vacuum the database into a new one, making an atomic snapshot
                statedb
                    .execute(
                        "vacuum into ?",
                        [statedb_backup_path
                            .to_str()
                            .ok_or_else(|| anyhow::format_err!("Non-UTF8 string"))?],
                    )
                    .await?;
                // Read the newly backed up database file
                let statedb_backup = tokio::fs::read(statedb_backup_path).await?;
                // Zstd compress it
                let compressed = zstd::encode_all(&statedb_backup[..], 0)?;

                // Push the archive to s3
                let stream_did = &stream.did;
                let name = format!("{BUCKET_STREAMS_DIR}/{stream_did}/{STATEDB_FILENAME}");
                bucket.put(name, &compressed).await?;

                // Record the backup status for the state DB
                self.set_stream_backed_up(
                    stream.did.clone(),
                    BackupUpdate::Stream(StreamUpdate::StateChanged),
                )
                .await?;
            }
        }

        Ok(())
    }

    pub async fn restore_from_s3_backup(&self, _config: &S3BackupConfig) -> anyhow::Result<()> {
        todo!()
    }
}

/// An update to the backed up status of a stream.
enum BackupUpdate {
    /// We have backed up a [`StreamUpdate`].
    Stream(StreamUpdate),
    /// We have backed up the stream metadata.
    Metadata {
        owners: Vec<String>,
        module_cid: Option<Cid>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct EventArchive {
    events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Debug)]
struct StreamMetadata {
    did: Did,
    did_key: StreamMetadataDidKey,
    owners: Vec<String>,
    module_cid: Option<Cid>,
}

#[derive(Serialize, Deserialize, Debug, ZeroizeOnDrop)]
enum StreamMetadataDidKey {
    #[serde(with = "dasl::drisl::serde_bytes")]
    P256([u8; 32]),
    #[serde(with = "dasl::drisl::serde_bytes")]
    K256([u8; 32]),
}

impl From<SigningKey> for StreamMetadataDidKey {
    fn from(value: SigningKey) -> Self {
        match value {
            SigningKey::P256(ref key) => Self::P256(key.to_bytes().into()),
            SigningKey::K256(ref key) => Self::K256(key.to_bytes().into()),
        }
    }
}

#[instrument(skip(db))]
pub async fn run_database_migrations(db: &Connection) -> anyhow::Result<()> {
    db.execute_transactional_batch(include_str!("schema.sql"))
        .await?;
    Ok(())
}

pub fn start_background_tasks() {
    // Garbage collect modules periodically
    tokio::spawn(async move {
        loop {
            STORAGE.garbage_collect_modules().await.ok();
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
