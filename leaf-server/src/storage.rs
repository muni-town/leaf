use std::{
    collections::HashSet,
    io::{Read, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, LazyLock, Weak},
    time::Duration,
};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use blake3::Hash;
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use leaf_stream::{
    LeafModule, StreamGenesis,
    types::{Event, LeafModuleDef},
    validate_wasm,
};
use leaf_utils::convert::{FromRows, ParseRow, ParseRows};
use libsql::Connection;
use parity_scale_codec::{Decode, Encode};
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

static MODULES: LazyLock<RwLock<WeakValueHashMap<Hash, Weak<LeafModule>>>> =
    LazyLock::new(|| RwLock::new(WeakValueHashMap::new()));

pub static BUCKET_MODULE_DIR: &str = "modules";
pub static BUCKET_STREAMS_DIR: &str = "streams";

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

#[derive(Debug, Clone)]
pub struct ListStreamsItem {
    pub id: Hash,
    pub latest_event: Option<i64>,
    pub genesis: StreamGenesis,
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
    #[instrument(skip(self, hash), err)]
    pub async fn has_wasm_blob(&self, hash: Hash) -> anyhow::Result<bool> {
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

    // Get a WASM blob from the database
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

    /// List the WASM blobs in the database
    pub async fn list_wasm_blobs(&self) -> anyhow::Result<HashSet<Hash>> {
        let modules: Vec<Hash> = self
            .db()
            .await
            .query("select hash from wasm_blobs", ())
            .await?
            .parse_rows()
            .await?;
        Ok(modules.into_iter().collect())
    }

    /// Get a module from the database
    pub async fn get_module(
        &self,
        module_def: LeafModuleDef,
    ) -> anyhow::Result<Option<Arc<LeafModule>>> {
        let modules = MODULES.upgradable_read().await;
        let module_id = module_def.module_id_and_bytes().0;
        if let Some(module) = modules.get(&module_id) {
            return Ok(Some(module));
        }

        let wasm = if let Some(id) = module_def.wasm_module {
            let id = Hash::from_bytes(id);
            Some(
                self.get_wasm_blob(id)
                    .await?
                    .ok_or_else(|| anyhow::format_err!("WASM requested by module not found"))?,
            )
        } else {
            None
        };
        let module = Arc::new(LeafModule::new(module_def, wasm.as_deref())?);

        let mut modules = RwLockUpgradableReadGuard::upgrade(modules).await;
        modules.insert(module_id, module.clone());

        Ok(Some(module))
    }

    /// Create a new stream
    #[instrument(skip(self), err)]
    pub async fn create_stream(&self, genesis: StreamGenesis) -> anyhow::Result<StreamHandle> {
        let (id, genesis_bytes) = genesis.get_stream_id_and_bytes();
        let creator = genesis.creator.clone();
        let (_id, module_def) = genesis.module.module_id_and_bytes();
        let stream = STREAMS.load(genesis).await?;

        self.db()
            .await
            .execute(
                "insert into streams (id, creator, genesis, module_def) \
                values (:id, :creator, :genesis, :module_def)",
                (
                    (":id", id.as_bytes().to_vec()),
                    (":creator", creator),
                    (":genesis", genesis_bytes),
                    (":module_def", module_def),
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
    pub async fn list_streams(&self) -> anyhow::Result<Vec<ListStreamsItem>> {
        let rows: Vec<(Hash, Option<i64>, Vec<u8>)> = self
            .db()
            .await
            .query("select id, latest_event, genesis from streams", ())
            .await?
            .parse_rows()
            .await?;
        let items = rows
            .into_iter()
            .map(|x| {
                Ok::<_, anyhow::Error>(ListStreamsItem {
                    id: x.0,
                    latest_event: x.1,
                    genesis: StreamGenesis::decode(&mut &x.2[..])?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(items)
    }

    /// Update the recorded current module for the stream in the leaf database.
    ///
    /// This should be called after a module change has been made to the stream so that the database
    /// can avoid garbage collecting the new WASM module that it depends on.
    #[instrument(skip(self), err)]
    pub async fn update_stream_wasm_module(
        &self,
        stream: Hash,
        wasm_module_hash: Option<Hash>,
    ) -> anyhow::Result<()> {
        self.db()
            .await
            .execute(
                "update streams set wasm_module_hash = :module where id = :id",
                (
                    (":module", wasm_module_hash.map(|x| x.as_bytes().to_vec())),
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
    async fn add_wasm_blob(&self, data: Vec<u8>) -> anyhow::Result<Hash> {
        let hash = blake3::hash(&data);
        self.db()
            .await
            .execute(
                r#"insert or ignore into wasm_blobs (hash, data) values (:hash, :data)"#,
                ((":hash", hash.as_bytes().to_vec()), (":data", data)),
            )
            .await?;
        Ok(hash)
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
    pub async fn garbage_collect_wasm_and_modules(&self) -> anyhow::Result<()> {
        let mut deleted = self
            .db()
            .await
            .execute_batch(
                r#"
                delete from staged_wasm where (unixepoch() - timestamp) > 500 returning creator, hash;
                delete from wasm_blobs where not exists (
                    select 1 from staged_wasm s where s.hash=wasm_blobs.hash
                        union
                    select 1 from streams s where s.wasm_module_hash=wasm_blobs.hash
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

        anyhow::bail!(
            "TODO: s3 backup not implemented yet. Modules are not properly included in the archive yet"
        );

        // List wasm modules in the bucket
        let modules_in_backup = bucket
            .list(BUCKET_MODULE_DIR, None)
            .await?
            .into_iter()
            .flat_map(|x| x.contents.into_iter())
            .filter_map(|x| {
                Hash::from_hex(
                    x.key
                        .strip_prefix(&format!("{BUCKET_MODULE_DIR}/"))
                        .unwrap_or_default()
                        .strip_suffix(".wasm.gz")
                        .unwrap_or_default(),
                )
                .ok()
            })
            .collect::<HashSet<_>>();

        // List wasm modules we have locally
        let modules_local = self.list_wasm_blobs().await?;

        // Upload local modules not in backup
        for module_not_in_backup in modules_local.difference(&modules_in_backup) {
            let Some(module) = self.get_wasm_blob(*module_not_in_backup).await? else {
                continue;
            };
            let mut compressor = GzEncoder::new(Vec::new(), Compression::default());
            compressor.write_all(&module)?;
            let compressed = compressor.finish()?;
            bucket
                .put(
                    format!("{BUCKET_MODULE_DIR}/{module_not_in_backup}.wasm.gz"),
                    &compressed,
                )
                .await?;
        }

        // Download backup modules that aren't local
        for module_not_local in modules_in_backup.difference(&modules_local) {
            let resp = bucket
                .get(format!("{BUCKET_MODULE_DIR}/{module_not_local}.wasm.gz"))
                .await?;
            let resp_bytes = resp.bytes().await?;
            let mut decompressed = Vec::new();
            let mut decompressor = GzDecoder::new(&resp_bytes[..]);
            decompressor.read_to_end(&mut decompressed)?;
            let hash = blake3::hash(&decompressed);
            if hash == *module_not_local {
                self.add_wasm_blob(decompressed).await?;
            } else {
                tracing::error!(
                    actual_hash=%hash,
                    hash_in_filename=%module_not_local,
                    "Error importing WASM module from s3: module hash did not match filename."
                );
            }
        }

        // Get the list of streams we have on the server
        let streams_local = self.list_streams().await?;
        let stream_ids_local = streams_local.iter().map(|x| x.id).collect::<HashSet<_>>();

        // Get the list of streams that exist on S3
        let streams_dir = format!("{BUCKET_STREAMS_DIR}/");
        let stream_ids_on_s3 = bucket
            .list(&streams_dir, Some("/"))
            .await?
            .into_iter()
            .flat_map(|x| {
                x.common_prefixes.into_iter().flat_map(|x| {
                    x.into_iter().filter_map(|x| {
                        x.prefix
                            .strip_prefix(&streams_dir)
                            .and_then(|x| x.strip_suffix("/"))
                            .map(|x| x.to_owned())
                    })
                })
            })
            .flat_map(|x| Hash::from_str(&x).ok())
            .collect::<HashSet<_>>();

        // For each stream we have locally
        for stream in streams_local {
            // Get the latest event we have locally. The `0` means that all we have is the genesis,
            // aka. an empty stream.
            let latest_event_local = stream.latest_event.unwrap_or(0);

            // Get the latest event from the S3 bucket
            let stream_dir = format!("{BUCKET_STREAMS_DIR}/{}", stream.id);
            let ranges_on_s3 = bucket
                .list(&stream_dir, None)
                .await?
                .into_iter()
                // We get the list of pages
                .flat_map(|x| x.contents.into_iter())
                .filter_map(|x| {
                    // Find the name of the stream
                    let name = x
                        .key
                        .strip_prefix(&format!("{stream_dir}/"))
                        .unwrap_or_default();
                    // And parse the [start]-[end].lvg.gz filename
                    let (start, end) =
                        name.strip_suffix(".lvt.gz").unwrap_or("").split_once('-')?;
                    // And grab start and end
                    let start = start.parse::<i64>().ok()?;
                    let end = end.parse::<i64>().ok()?;
                    Some((start, end))
                })
                .collect::<Vec<_>>();
            let latest_in_s3 = ranges_on_s3.iter().map(|x| x.1).max();

            // Check if we need to send new events to S3:
            // - If there is a latest s3 event, then we need to update it if it is less than our
            // local latest event.
            // - If there is no latest event on s3, then we need to send the genesis and latest
            //   events that we have locally.
            if latest_in_s3.map(|l| l < latest_event_local).unwrap_or(true) {
                // Open the local stream
                let Some(s) = self.open_stream(stream.id).await? else {
                    continue;
                };
                // Get the events that are newer than the latest s3 event.
                let events = s
                    .raw_get_events((latest_in_s3.map(|l| l + 1).unwrap_or(1))..)
                    .await?;
                let events_len = events.len() as i64;

                // If there is nothing on S3 yet, then we need to also send the genesis
                let genesis = if latest_in_s3.is_none() {
                    Some(stream.genesis)
                } else {
                    None
                };

                if events_len == 0 && latest_in_s3.is_some() {
                    // This should be unreachable, but skip this just in case
                    tracing::error!(?latest_in_s3, "Hit unreachable condition: ignoring.");
                    continue;
                }

                // Create and compress the event archive
                let archive = EventArchive { genesis, events };
                let archive_bytes = archive.encode();
                let mut compressor = GzEncoder::new(Vec::new(), Compression::default());
                compressor.write_all(&archive_bytes)?;
                let compressed = compressor.finish()?;

                // Push the archive to s3
                let name = format!(
                    "{BUCKET_STREAMS_DIR}/{}/{}-{}.lvt.gz",
                    stream.id,
                    latest_in_s3.map(|l| l + 1).unwrap_or(0),
                    latest_in_s3.map(|l| l + events_len).unwrap_or(events_len)
                );
                bucket.put(name, &compressed).await?;

            // If s3 has a newer event than we have locally.
            } else if latest_in_s3
                .map(|l| l > latest_event_local)
                .unwrap_or(false)
            {
                // For now just log a warning. This shouldn't normally happen, we expect either the
                // local stream to be completely empty or newer than s3. Having s3 newer means there
                // is some kind of corruption or an out-of-date local database which shouldn't
                // happen under normal circumstances.
                tracing::warn!(
                    %stream.id,
                    "There is newer backup data available on S3 than there is locally for a stream. \
                    This should not normally happen so the s3 backup will not be restored. This should \
                    be investigated manually."
                )
            }
        }

        // Get the list of streams that are on S3, but not local. These will need to be created.
        let stream_ids_on_s3_but_not_local = stream_ids_on_s3.difference(&stream_ids_local);

        'stream: for stream_id in stream_ids_on_s3_but_not_local {
            tracing::info!(
                stream.id=%stream_id,
                "Found stream in S3 backup that is not local. Attempting to import."
            );

            let stream_dir = format!("{BUCKET_STREAMS_DIR}/{}", stream_id);
            let mut ranges_on_s3 = bucket
                .list(&stream_dir, None)
                .await?
                .into_iter()
                // We get the list of pages
                .flat_map(|x| x.contents.into_iter())
                .filter_map(|x| {
                    // Find the name of the stream
                    let name = x
                        .key
                        .strip_prefix(&format!("{stream_dir}/"))
                        .unwrap_or_default();
                    // And parse the [start]-[end].lvt.gz filename
                    let (start, end) =
                        name.strip_suffix(".lvt.gz").unwrap_or("").split_once('-')?;
                    // And grab start and end
                    let start = start.parse::<i64>().ok()?;
                    let end = end.parse::<i64>().ok()?;
                    Some((start, end))
                })
                .collect::<Vec<_>>();
            ranges_on_s3.sort_by(|x, y| x.0.cmp(&y.0));

            // Make sure ranges are non-overlapping and without holes
            let mut last_idx = -1;
            for (start, end) in &ranges_on_s3 {
                if *start != last_idx + 1 {
                    tracing::warn!(
                        stream.id=%stream_id,
                        "Ranges of events on s3 are not continuous, skipping import."
                    );
                    continue 'stream;
                }
                last_idx = *end;
            }

            // Download each archive and apply it to the stream
            let count = ranges_on_s3.len();
            let mut stream = None;
            for (i, (start, end)) in ranges_on_s3.into_iter().enumerate() {
                let is_last_range = i + 1 == count;
                let archive_path = format!(
                    "{BUCKET_STREAMS_DIR}/{}/{}-{}.lvt.gz",
                    stream_id, start, end
                );
                let resp = bucket.get(archive_path).await?;
                let bytes = resp.bytes().await?;
                let mut decompressed = Vec::new();
                let mut decompressor = GzDecoder::new(&bytes[..]);
                decompressor.read_to_end(&mut decompressed)?;
                let archive = EventArchive::decode(&mut &decompressed[..])?;

                // If this is the initial archive
                if start == 0 {
                    // Get the genesis info
                    let Some(genesis) = archive.genesis else {
                        tracing::error!(
                            stream.id=%stream_id,
                            "Cannot import: first event batch does not contain the stream genesis."
                        );
                        continue 'stream;
                    };

                    // Create the stream
                    let s = self.create_stream(genesis).await?;
                    if s.id() != *stream_id {
                        tracing::error!(
                            expected_stream_id=%stream_id,
                            imported_stream_id=%s.id(),
                            "Imported stream genesis but got different sream ID than expected. \
                            Aborting import after having created new stream with unexpected ID."
                        );
                        continue 'stream;
                    }
                    stream = Some(s);
                }

                // Get the stream
                let Some(s) = &stream else {
                    tracing::error!("Stream backup missing genesis");
                    continue 'stream;
                };

                // Import the events from the archive
                s.raw_import_events(archive.events).await?;

                // Set the module and catch it up
                if is_last_range {
                    // FIXME： we need the module definition to be included here.
                    // s.raw_set_module(archive.module_def).await?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Decode, Encode, Debug)]
struct EventArchive {
    // FIXME： we need the module definition to be included here.
    genesis: Option<StreamGenesis>,
    events: Vec<Event>,
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
            STORAGE.garbage_collect_wasm_and_modules().await.ok();
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
