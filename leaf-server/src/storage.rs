use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Weak},
    time::Duration,
};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use leaf_stream::{
    BasicModule, LeafModule,
    atproto_plc::{Did, SigningKey},
    dasl::cid::{Cid, Codec::Drisl},
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

use crate::{
    async_oncelock::AsyncOnceLock,
    streams::{STREAMS, StreamHandle},
};

pub static GLOBAL_SQLITE_PRAGMA: &str = "pragma synchronous = normal; pragma journal_mode = wal;";

pub static STORAGE: LazyLock<Storage> = LazyLock::new(Storage::default);

static MODULES: LazyLock<RwLock<WeakValueHashMap<Cid, Weak<dyn LeafModule>>>> =
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
    pub id: Did,
    pub module_cid: Option<Cid>,
    pub latest_event: Option<i64>,
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
        latest_event: Option<i64>,
    ) -> anyhow::Result<()> {
        let db = self.db().await;
        db.execute(
            "update backup_status set updated_auth = unixepoch() where did = ?",
            [stream_did.as_str().to_string()],
        )
        .await?;
        if let Some(latest_event) = latest_event {
            db.execute(
                "update streams set latest_event = ? where did = ?",
                (latest_event, stream_did.as_str().to_string()),
            )
            .await?;
        }
        Ok(())
    }

    /// Get the list of streams and the latest event we have for each
    pub async fn list_streams(&self) -> anyhow::Result<Vec<ListStreamsItem>> {
        let rows: Vec<(Did, Option<Cid>, Option<i64>)> = self
            .db()
            .await
            .query("select did, module_cid, latest_event from streams", ())
            .await?
            .parse_rows()
            .await?;
        let items = rows
            .into_iter()
            .map(|x| {
                Ok::<_, anyhow::Error>(ListStreamsItem {
                    id: x.0,
                    module_cid: x.1,
                    latest_event: x.2,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
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
        // FIXME: reimplement S3 backups
        Ok(())
        // let Some(bucket) = &*self.backup_bucket.read().await else {
        //     return Ok(());
        // };

        // // List modules in the bucket
        // let modules_in_backup = bucket
        //     .list(BUCKET_MODULE_DIR, None)
        //     .await?
        //     .into_iter()
        //     .flat_map(|x| x.contents.into_iter())
        //     .filter_map(|x| {
        //         Cid::from_str(
        //             x.key
        //                 .strip_prefix(&format!("{BUCKET_MODULE_DIR}/"))
        //                 .unwrap_or_default()
        //                 .strip_suffix(".module.zstd")
        //                 .unwrap_or_default(),
        //         )
        //         .ok()
        //     })
        //     .collect::<HashSet<_>>();

        // // List modules we have locally
        // let modules_local = self.list_module_blobs().await?;

        // // Upload local modules not in backup
        // for module_not_in_backup in modules_local.difference(&modules_in_backup) {
        //     let Some(module) = self.get_module_blob(*module_not_in_backup).await? else {
        //         continue;
        //     };
        //     let compressed = zstd::encode_all(&module[..], 0)?;
        //     bucket
        //         .put(
        //             format!("{BUCKET_MODULE_DIR}/{module_not_in_backup}.module.zstd"),
        //             &compressed,
        //         )
        //         .await?;
        // }

        // // Download backup modules that aren't local
        // for module_not_local in modules_in_backup.difference(&modules_local) {
        //     let resp = bucket
        //         .get(format!(
        //             "{BUCKET_MODULE_DIR}/{module_not_local}.module.zstd"
        //         ))
        //         .await?;
        //     let resp_bytes = resp.bytes().await?;
        //     let decompressed = zstd::decode_all(&resp_bytes[..])?;
        //     let cid = Cid::digest_sha2(Drisl, &decompressed);
        //     if cid == *module_not_local {
        //         self.add_module_blob(decompressed).await?;
        //     } else {
        //         tracing::error!(
        //             actual_hash=%cid,
        //             hash_in_filename=%module_not_local,
        //             "Error importing module from s3: module hash did not match filename."
        //         );
        //     }
        // }

        // // Get the list of streams we have on the server
        // let streams_local = self.list_streams().await?;
        // let stream_dids_local = streams_local.iter().map(|x| x.id).collect::<HashSet<_>>();

        // // Get the list of streams that exist on S3
        // let streams_dir = format!("{BUCKET_STREAMS_DIR}/");
        // let stream_dids_on_s3 = bucket
        //     .list(&streams_dir, Some("/"))
        //     .await?
        //     .into_iter()
        //     .flat_map(|x| {
        //         x.common_prefixes.into_iter().flat_map(|x| {
        //             x.into_iter().filter_map(|x| {
        //                 x.prefix
        //                     .strip_prefix(&streams_dir)
        //                     .and_then(|x| x.strip_suffix("/"))
        //                     .map(|x| x.to_owned())
        //             })
        //         })
        //     })
        //     .flat_map(|x| Cid::from_str(&x).ok())
        //     .collect::<HashSet<_>>();

        // // For each stream we have locally
        // for stream in streams_local {
        //     // Get the latest event we have locally. The `0` means that all we have is the genesis,
        //     // aka. an empty stream.
        //     let latest_event_local = stream.latest_event.unwrap_or(0);

        //     // Get the latest event from the S3 bucket
        //     let stream_dir = format!("{BUCKET_STREAMS_DIR}/{}", stream.id);
        //     let ranges_on_s3 = bucket
        //         .list(&stream_dir, None)
        //         .await?
        //         .into_iter()
        //         // We get the list of pages
        //         .flat_map(|x| x.contents.into_iter())
        //         .filter_map(|x| {
        //             // Find the name of the stream
        //             let name = x
        //                 .key
        //                 .strip_prefix(&format!("{stream_dir}/"))
        //                 .unwrap_or_default();
        //             // And parse the [start]-[end].lvg.zstd filename
        //             let (start, end) = name
        //                 .strip_suffix(".lvt.zstd")
        //                 .unwrap_or("")
        //                 .split_once('-')?;
        //             // And grab start and end
        //             let start = start.parse::<i64>().ok()?;
        //             let end = end.parse::<i64>().ok()?;
        //             Some((start, end))
        //         })
        //         .collect::<Vec<_>>();
        //     let latest_in_s3 = ranges_on_s3.iter().map(|x| x.1).max();

        //     // Check if we need to send new events to S3:
        //     // - If there is a latest s3 event, then we need to update it if it is less than our
        //     // local latest event.
        //     // - If there is no latest event on s3, then we need to send the genesis and latest
        //     //   events that we have locally.
        //     if latest_in_s3.map(|l| l < latest_event_local).unwrap_or(true) {
        //         // Open the local stream
        //         let s = STREAMS.load(stream.id).await?;

        //         // Get the events that are newer than the latest s3 event.
        //         let events = s
        //             .raw_get_events((latest_in_s3.map(|l| l + 1).unwrap_or(1))..)
        //             .await?;
        //         let events_len = events.len() as i64;

        //         if events_len == 0 && latest_in_s3.is_some() {
        //             // This should be unreachable, but skip this just in case
        //             tracing::error!(?latest_in_s3, "Hit unreachable condition: ignoring.");
        //             continue;
        //         }

        //         // Create and compress the event archive
        //         let archive = EventArchive { genesis, events };
        //         let archive_bytes = archive.encode();
        //         let compressed = zstd::encode_all(&archive_bytes[..], 0)?;

        //         // Push the archive to s3
        //         let name = format!(
        //             "{BUCKET_STREAMS_DIR}/{}/{}-{}.lvt.zstd",
        //             s.id,
        //             latest_in_s3.map(|l| l + 1).unwrap_or(0),
        //             latest_in_s3.map(|l| l + events_len).unwrap_or(events_len)
        //         );
        //         bucket.put(name, &compressed).await?;

        //     // If s3 has a newer event than we have locally.
        //     } else if latest_in_s3
        //         .map(|l| l > latest_event_local)
        //         .unwrap_or(false)
        //     {
        //         // For now just log a warning. This shouldn't normally happen, we expect either the
        //         // local stream to be completely empty or newer than s3. Having s3 newer means there
        //         // is some kind of corruption or an out-of-date local database which shouldn't
        //         // happen under normal circumstances.
        //         tracing::warn!(
        //             %stream.id,
        //             "There is newer backup data available on S3 than there is locally for a stream. \
        //             This should not normally happen so the s3 backup will not be restored. This should \
        //             be investigated manually."
        //         )
        //     }
        // }

        // // Get the list of streams that are on S3, but not local. These will need to be created.
        // let stream_dids_on_s3_but_not_local = stream_dids_on_s3.difference(&stream_dids_local);

        // 'stream: for stream_did in stream_dids_on_s3_but_not_local {
        //     tracing::info!(
        //         stream.id=%stream_did,
        //         "Found stream in S3 backup that is not local. Attempting to import."
        //     );

        //     let stream_dir = format!("{BUCKET_STREAMS_DIR}/{}", stream_did);
        //     let mut ranges_on_s3 = bucket
        //         .list(&stream_dir, None)
        //         .await?
        //         .into_iter()
        //         // We get the list of pages
        //         .flat_map(|x| x.contents.into_iter())
        //         .filter_map(|x| {
        //             // Find the name of the stream
        //             let name = x
        //                 .key
        //                 .strip_prefix(&format!("{stream_dir}/"))
        //                 .unwrap_or_default();
        //             // And parse the [start]-[end].lvt.zstd filename
        //             let (start, end) = name
        //                 .strip_suffix(".lvt.zstd")
        //                 .unwrap_or("")
        //                 .split_once('-')?;
        //             // And grab start and end
        //             let start = start.parse::<i64>().ok()?;
        //             let end = end.parse::<i64>().ok()?;
        //             Some((start, end))
        //         })
        //         .collect::<Vec<_>>();
        //     ranges_on_s3.sort_by(|x, y| x.0.cmp(&y.0));

        //     // Make sure ranges are non-overlapping and without holes
        //     let mut last_idx = -1;
        //     for (start, end) in &ranges_on_s3 {
        //         if *start != last_idx + 1 {
        //             tracing::warn!(
        //                 stream.id=%stream_did,
        //                 "Ranges of events on s3 are not continuous, skipping import."
        //             );
        //             continue 'stream;
        //         }
        //         last_idx = *end;
        //     }

        //     // Download each archive and apply it to the stream
        //     let count = ranges_on_s3.len();
        //     let mut stream = None;
        //     for (i, (start, end)) in ranges_on_s3.into_iter().enumerate() {
        //         let is_last_range = i + 1 == count;
        //         let archive_path = format!(
        //             "{BUCKET_STREAMS_DIR}/{}/{}-{}.lvt.zstd",
        //             stream_did, start, end
        //         );
        //         let resp = bucket.get(archive_path).await?;
        //         let bytes = resp.bytes().await?;
        //         let decompressed = zstd::decode_all(&bytes[..])?;
        //         let archive = EventArchive::decode(&mut &decompressed[..])?;

        //         // If this is the initial archive
        //         if start == 0 {
        //             // Get the genesis info
        //             let Some(genesis) = archive.genesis else {
        //                 tracing::error!(
        //                     stream.id=%stream_did,
        //                     "Cannot import: first event batch does not contain the stream genesis."
        //                 );
        //                 continue 'stream;
        //             };

        //             // Create the stream
        //             let s = self.create_stream(genesis).await?;
        //             if s.id() != *stream_did {
        //                 tracing::error!(
        //                     expected_stream_did=%stream_did,
        //                     imported_stream_did=%s.id(),
        //                     "Imported stream genesis but got different sream ID than expected. \
        //                     Aborting import after having created new stream with unexpected ID."
        //                 );
        //                 continue 'stream;
        //             }
        //             stream = Some(s);
        //         }

        //         // Get the stream
        //         let Some(s) = &stream else {
        //             tracing::error!("Stream backup missing genesis");
        //             continue 'stream;
        //         };

        //         // Import the events from the archive
        //         s.raw_import_events(archive.events).await?;

        //         // Set the module and catch it up
        //         if is_last_range {
        //             // FIXME： we need the module definition to be included here.
        //             // s.raw_set_module(archive.module_def).await?;
        //         }
        //     }
        // }

        // Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct EventArchive {
    events: Vec<Event>,
}

#[derive(Serialize, Deserialize, Debug)]
struct StreamMetadata {
    owner: String,
    module_cid: Cid,
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
