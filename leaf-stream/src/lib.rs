use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use anyhow::Context;
use async_lock::{Mutex, RwLock};
use blake3::Hash;
use leaf_stream_types::{
    Decode, Event, IncomingEvent, LeafModuleDef, LeafQuery, LeafQueryReponse, LeafSubscribeQuery,
    QueryValidationError, SqlRow, SqlRows, SqlValue,
};
use leaf_utils::convert::*;
use libsql::{AuthAction, Authorization, Connection, ScalarFunctionDef};
use parity_scale_codec::Encode;
use tracing::instrument;
use ulid::Ulid;

pub type SubscriptionResultReceiver =
    async_channel::Receiver<Result<LeafQueryReponse, StreamError>>;
pub type SubscriptionResultSender = async_channel::Sender<Result<LeafQueryReponse, StreamError>>;

pub use module::*;
mod module;

pub mod encoding;
use encoding::Encodable;

pub use blake3;
pub use leaf_stream_types as types;
pub use libsql;
pub use ulid;

/// The genesis configuration of an event stream.
#[derive(Encode, Decode, Debug, Clone)]
pub struct StreamGenesis {
    /// A ULID, which encompasses the timestamp and additional randomness, included in this stream
    /// to make it's hash unique.
    ///
    /// Note that this is not the stream ID, which is computed from the hash of the
    /// [`GenesisStreamConfig`].
    pub stamp: Encodable<Ulid>,
    /// User ID of the user that created the stream.
    pub creator: String,
    /// The initial module definition for the stream.
    pub module: LeafModuleDef,
    /// If this is `true` it means that module updates must be made by the module's materializer.
    ///
    /// If this is `false`, the module may also be updated at any time by the user that created the
    /// stream.
    pub strict_module_updates: bool,
}

impl StreamGenesis {
    /// Compute the stream ID of this stream based on it's genesis config.
    pub fn get_stream_id_and_bytes(&self) -> (Hash, Vec<u8>) {
        let encoded = self.encode();
        (blake3::hash(&encoded), encoded)
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    info: Arc<StreamInfo>,
    state: Arc<RwLock<StreamState>>,
}

#[derive(Debug, Clone)]
struct StreamInfo {
    id: Hash,
    genesis: StreamGenesis,
}

#[derive(Debug)]
struct StreamState {
    db_filename: String,
    db: libsql::Connection,
    module_state: ModuleState,
    latest_event: i64,
    module_event_cursor: i64,
    query_subscriptions: Arc<Mutex<Vec<ActiveSubscription>>>,
    latest_event_subscriptions: Arc<Mutex<Vec<async_channel::Sender<i64>>>>,
    worker_sender: Option<async_channel::Sender<WorkerMessage>>,
}

enum WorkerMessage {
    /// We have new events: all subscriptions should be updated.
    NewEvents { latest_event: i64 },
    /// A specific subscription needs to be updated.
    NeedsUpdate(Ulid),
}

#[derive(Debug, Clone)]
struct ActiveSubscription {
    pub sub_id: Ulid,
    pub subscription_query: LeafSubscribeQuery,
    pub latest_event: i64,
    pub result_sender: SubscriptionResultSender,
}

enum ModuleState {
    Unloaded(Hash),
    Loaded {
        module: Arc<LeafModule>,
        module_db: libsql::Connection,
    },
}
impl ModuleState {
    fn id(&self) -> Hash {
        match self {
            ModuleState::Unloaded(hash) => *hash,
            ModuleState::Loaded { module, .. } => module.id(),
        }
    }
}

impl std::fmt::Debug for ModuleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unloaded(arg0) => f.debug_tuple("Unloaded").field(arg0).finish(),
            Self::Loaded { .. } => f.debug_tuple("Loaded").field(&"dyn LeafModule").finish(),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("LibSQL error: {0}")]
    Libsql(#[from] libsql::Error),
    #[error(
        "The opened database is for a stream with a different ID. \
        The expected ID was {expected_id} but the ID in the database was {database_id}."
    )]
    IdMismatch {
        expected_id: Hash,
        database_id: Hash,
    },
    #[error("Attempted to provide module for stream when it was not needed.")]
    ModuleNotNeeded,
    #[error("The stream's module has not been provided: {0}")]
    ModuleNotProvided(Hash),
    #[error(
        "Attempted to provide module with different ID than the one needed \
        by the stream. Needed {needed} but got {provided}"
    )]
    InvalidModuleId { needed: Hash, provided: Hash },
    #[error("Event was rejected by filter module: {reason}")]
    EventRejected { reason: String },
    #[error("Error while running stream module: {0}")]
    ModuleError(#[from] anyhow::Error),
    #[error("The module DB must have an attachment to the events DB under the 'events' name.")]
    ModuleDbMissingEventsAttachment,
    #[error(
        "The module DB's attached 'events' database does not have the same filename as the events database for the stream."
    )]
    ModuleDbEventsAttachmentHasWrongFilename,
    #[error(
        "Could not query main database file. This should not happen under normal circumstances."
    )]
    FailedToQueryDatabaseFilename,
    #[error("The query `{0}` does not exist in this module.")]
    QueryDoesNotExistInModule(String),
    #[error("Error validating query: {0}")]
    QueryValidationError(#[from] QueryValidationError),
}

impl Stream {
    pub fn id(&self) -> blake3::Hash {
        self.info.id
    }

    pub fn genesis(&self) -> &StreamGenesis {
        &self.info.genesis
    }

    pub async fn latest_event(&self) -> i64 {
        self.state.read().await.latest_event
    }

    pub async fn module_id(&self) -> Hash {
        self.state.read().await.module_state.id()
    }

    /// This is a raw method to set the current module of the stream, without any other processing.
    /// You usually should not use this, but you may need it if you are, for example, importing the
    /// stream from a backup.
    pub async fn raw_set_module(&self, module_id: Hash) -> anyhow::Result<()> {
        let mut state = self.state.write().await;
        state
            .db
            .execute(
                "update stream_state set module = ?, module_event_cursor = null where id = 1",
                [module_id.as_bytes().to_vec()],
            )
            .await
            .context("error updating steam module and module event cursor")?;
        state.module_event_cursor = 0;
        state.module_state = ModuleState::Unloaded(module_id);

        Ok(())
    }

    /// Open a stream.
    #[instrument(skip(db))]
    pub async fn open(genesis: StreamGenesis, db: libsql::Connection) -> Result<Self, StreamError> {
        let id = genesis.get_stream_id_and_bytes().0;

        // run database migrations
        db.execute_batch(include_str!("./streamdb_schema_00.sql"))
            .await?;

        // Get the latest event index from the database
        let latest_event = db
            .query("select max(id) from events;", ())
            .await?
            .next()
            .await
            .context("error querying latest event")?;
        let latest_event = if let Some(row) = latest_event {
            Option::<i64>::from_row(row)
                .await
                .context("error parsing latest event")?
                .unwrap_or(0)
        } else {
            0
        };

        // Load the stream state from the database
        let row = db
            .query(
                "select stream_id, module, params, module_event_cursor \
                from stream_state where id=1",
                (),
            )
            .await
            .context("error querying stream state")?
            .next()
            .await
            .context("error loading stream state from query")?;

        // Parse the current state from the database or initialize a new state if one does not
        // exist.
        let module;
        let module_event_cursor;
        if let Some(row) = row {
            let (db_stream_id, db_module, db_module_event_cursor): (Hash, Hash, Option<i64>) = row
                .parse_row()
                .await
                .context("error parsing stream state")?;
            if db_stream_id != id {
                return Err(StreamError::IdMismatch {
                    expected_id: id,
                    database_id: db_stream_id,
                });
            }
            module = ModuleState::Unloaded(db_module);
            module_event_cursor = db_module_event_cursor.unwrap_or(0);
        } else {
            let module_id = genesis.module.module_id_and_bytes().0;
            // Initialize the stream state
            db.execute(
                "insert into stream_state \
                (id, creator, stream_id, module_id, module_event_cursor) values \
                (1, :creator, :stream_id, :module, :params, null) ",
                (
                    (":stream_id", id.as_bytes().to_vec()),
                    (":creator", genesis.creator.clone()),
                    (":module", module_id.as_bytes().to_vec()),
                ),
            )
            .await
            .context("error initializing stream state")?;

            module = ModuleState::Unloaded(module_id);
            module_event_cursor = 0;
        };

        // Get the filename of the passed-in database connection so that we can make sure modules
        // are attached to the same file.
        let files: Vec<String> = db
            .query(
                "select file from pragma_database_list where name = 'main'",
                (),
            )
            .await?
            .parse_rows()
            .await?;
        let Some(db_filename) = files.first().cloned() else {
            return Err(StreamError::FailedToQueryDatabaseFilename);
        };

        Ok(Self {
            info: Arc::new(StreamInfo { id, genesis }),
            state: Arc::new(RwLock::new(StreamState {
                db_filename,
                db,
                module_state: module,
                latest_event,
                module_event_cursor,
                query_subscriptions: Default::default(),
                latest_event_subscriptions: Arc::new(Mutex::new(Vec::new())),
                worker_sender: None,
            })),
        })
    }

    /// Get a subscription to the latest event index in the stream.
    /// 
    /// Every time new events are added to this stream, the new latest event will be sent to the
    /// receiver.
    pub async fn subscribe_latest_event(&self) -> async_channel::Receiver<i64> {
        let state = self.state.read().await;
        let mut latest_event_subscriptions = state.latest_event_subscriptions.lock().await;

        let (sender, receiver) = async_channel::bounded(12);
        latest_event_subscriptions.push(sender);

        receiver
    }

    /// Subscribe to the result of a leaf query.
    pub async fn subscribe(
        &self,
        subscription_query: LeafSubscribeQuery,
    ) -> SubscriptionResultReceiver {
        let state = self.state.read().await;
        let mut subscriptions = state.query_subscriptions.lock().await;

        // Take the opportunity to clean up any closed subscriptions
        subscriptions.retain(|v| !v.result_sender.is_closed());

        let (sender, receiver) = async_channel::bounded(12);

        // Create a new subscription
        let sub_id = Ulid::new();
        subscriptions.push(ActiveSubscription {
            sub_id,
            latest_event: subscription_query.start.map(|s| s - 1).unwrap_or(0),
            subscription_query,
            result_sender: sender,
        });

        // Have the worker update the subscription
        state
            .worker_sender
            .as_ref()
            .map(|s| s.try_send(WorkerMessage::NeedsUpdate(sub_id)));

        // Return the receiver
        receiver
    }

    /// If this stream needs a Leaf module to be loaded before it can continue processing events,
    /// then this will return `Some(hash)`.
    ///
    /// You must then call [`provide_module()`][Self::provide_module] with the module in order to
    /// allow the stream to continue processing.
    pub async fn needs_module(&self) -> Option<blake3::Hash> {
        let state = self.state.read().await;
        match state.module_state {
            ModuleState::Unloaded(hash) => Some(hash),
            _ => None,
        }
    }

    /// Provide the stream it's module, if it is needed.
    pub async fn provide_module(
        &self,
        module: Arc<LeafModule>,
        module_db: libsql::Connection,
    ) -> Result<(), StreamError> {
        let mut state = self.state.write().await;
        match &state.module_state {
            ModuleState::Unloaded(hash) => {
                if module.id() == *hash {
                    // Make sure an `events` database is connected, which must be connected to the
                    // same database with the same virtual filesystem.
                    let files: Vec<String> = module_db
                        .query(
                            "select file from pragma_database_list where name = 'events'",
                            (),
                        )
                        .await?
                        .parse_rows()
                        .await?;
                    let Some(db_filename) = files.first() else {
                        return Err(StreamError::ModuleDbMissingEventsAttachment);
                    };
                    if db_filename != &state.db_filename {
                        return Err(StreamError::ModuleDbEventsAttachmentHasWrongFilename);
                    }

                    // Install our user-defined function
                    install_udfs(&module_db)?;

                    state.module_state = ModuleState::Loaded { module, module_db };
                    drop(state);

                    // Make sure we catch up the new module if it needs it.
                    self.raw_catch_up_module().await?;

                    Ok(())
                } else {
                    Err(StreamError::InvalidModuleId {
                        needed: *hash,
                        provided: module.id(),
                    })
                }
            }
            ModuleState::Loaded { .. } => Err(StreamError::ModuleNotNeeded),
        }
    }

    fn ensure_module_loaded(
        module: &ModuleState,
    ) -> Result<(&LeafModule, &Connection), StreamError> {
        match module {
            ModuleState::Unloaded(hash) => Err(StreamError::ModuleNotProvided(*hash)),
            ModuleState::Loaded { module, module_db } => Ok((module.as_ref(), module_db)),
        }
    }

    /// Make sure that the module has processed all of the events in the stream so far.
    ///
    /// Returns the number of events that were processed while catching up.
    ///
    /// > **Note:** You usually don't need to call this yourself, but in some cases, such as after
    /// > calling [`raw_import_events()`][Self::raw_import_events] and
    /// > [`raw_set_module()`][Self::raw_set_module].
    #[instrument(skip(self), err)]
    pub async fn raw_catch_up_module(&self) -> Result<i64, StreamError> {
        let mut state = self.state.write().await;
        let (module, module_db) = Self::ensure_module_loaded(&state.module_state)?;

        // NOTE the init sql must be idempodent since there are edge cases where this might be
        // called twice.
        if state.module_event_cursor == 0 {
            module_db.authorizer(Some(Arc::new(module_init_authorizer)))?;
            module_db.execute_batch(&module.def().init_sql).await?;
            module_db.authorizer(None)?;
        }

        assert!(
            state.latest_event >= state.module_event_cursor,
            "Somehow the module event cursor is higher than the latest event."
        );
        let events_behind = state.latest_event - state.module_event_cursor;
        if events_behind == 0 {
            return Ok(events_behind);
        }

        // Get all of the events that have not been applied by the module yet.
        let events: Vec<(i64, String, Vec<u8>)> = state
            .db
            .query(
                "select id, user, payload from events where id > ?",
                [state.module_event_cursor],
            )
            .await?
            .parse_rows()
            .await?;
        let even_count = events.len();

        // Start a new transaction
        module_db.execute("begin immediate", ()).await?;

        let result = async {
            for (id, user, payload) in events {
                assert_eq!(id, state.module_event_cursor + 1);

                // Setup the temporary `next_event` table to contain the next event to materialize.
                module_db
                    .execute("drop table if exists temp.next_event", ())
                    .await?;
                module_db.execute(
                    r#"
                        create temp table if not exists next_event as select (? as user, ? as payload)
                    "#,
                    (user, payload),
                ).await?;

                // Execute the materializer for the event
                module_db.authorizer(Some(Arc::new(module_materialize_authorizer)))?;
                module_db.execute_batch(&module.def().materializer).await?;
                module_db.authorizer(None)?;

                // Increment the event cursor
                module_db
                    .execute(
                        "update events.stream_state set module_event_cursor = ? where id = 1",
                        [state.module_event_cursor],
                    )
                    .await?;
            }
            anyhow::Ok(())
        }
        .await;

        // Handle errors by rolling back the transaction
        if let Err(e) = result {
            module_db.execute("rollback", ()).await?;
            return Err(e.into());
        } else {
            module_db.execute("commit", ()).await?;
            state.module_event_cursor += even_count as i64;
        }

        assert_eq!(
            state.latest_event, state.module_event_cursor,
            "Module event cursor still not caught up."
        );

        Ok(events_behind)
    }

    /// Attempt to add the batch of events to the stream.
    ///
    /// Either the whole batch of events will be added, or the entire batch will be rejected, making
    /// multiple events act as an atomic transaction.
    #[instrument(skip(self, events), err)]
    pub async fn add_events(
        &self,
        events: Vec<IncomingEvent>,
    ) -> Result<Option<Hash>, StreamError> {
        let mut state = self.state.write().await;
        let event_count = events.len();

        // Make sure the current module is caught up
        self.raw_catch_up_module()
            .await
            .context("error catching up module")?;

        // NOTE: we use a write lock here because we are starting a transaction and need to make
        // sure other async tasks don't come and try to use this same database connection while the
        // transaction is in progress.
        let (module, module_db) = Self::ensure_module_loaded(&state.module_state)?;

        // Start a new transaction
        module_db.execute("begin immediate", ()).await?;

        let result = async {
            for IncomingEvent { user, payload } in events {
                // Setup the temporary `next_event` table to contain the next event to authorize / materialize.
                module_db
                    .execute("drop table if exists temp.next_event", ())
                    .await?;
                module_db.execute(
                    r#"
                        create temp table if not exists next_event as select (? as user, ? as payload)
                    "#,
                    (user, payload),
                ).await?;

                // Execute the authorizer
                module_db.authorizer(Some(Arc::new(module_authorize_authorizer)))?;
                module_db.execute_batch(&module.def().authorizer).await?;
                module_db.authorizer(None)?;

                // Insert the event into the events table
                module_db.execute(
                    "insert into events.events select (user, payload) from next_event)",
                    ()
                ).await?;

                // Execute the materializer for the event
                module_db.authorizer(Some(Arc::new(module_materialize_authorizer)))?;
                module_db.execute_batch(&module.def().materializer).await?;
                module_db.authorizer(None)?;

                // Increment the module event cursor
                module_db
                    .execute(
                        "update events.stream_state set module_event_cursor = ? where id = 1",
                        [state.module_event_cursor],
                    )
                    .await?;
            }

            anyhow::Ok(())
        }.await;

        // Handle errors by rolling back the transaction
        if let Err(e) = result {
            module_db.execute("rollback", ()).await?;
            return Err(e.into());
        } else {
            module_db.execute("commit", ()).await?;
            state.latest_event += event_count as i64;
            state.module_event_cursor += event_count as i64;
        }

        // Update query subscriptions
        state.worker_sender.as_ref().map(|s| {
            s.try_send(WorkerMessage::NewEvents {
                latest_event: state.latest_event,
            })
            .ok()
        });

        // TODO: allow events in the module to update the module, but only if the event that updates
        // the module is the last event in the batch.

        Ok(None)
    }

    /// Get the provided range of events from the stream.
    ///
    /// Unlike [`fetch_events()`][Self::fetch_events], this skips the stream module and all access
    /// controls.
    ///
    /// This is useful, for example, when doing backups or other processing directly, as opposed to
    /// retrieving the data for a 3rd party.
    pub async fn raw_get_events<R: RangeBounds<i64>>(
        &self,
        range: R,
    ) -> Result<Vec<Event>, StreamError> {
        let state = self.state.read().await;
        let min = match range.start_bound() {
            Bound::Included(min) => *min,
            Bound::Excluded(x) => *x + 1,
            Bound::Unbounded => 1,
        };
        let max = match range.end_bound() {
            Bound::Included(max) => *max,
            Bound::Excluded(x) => *x - 1,
            Bound::Unbounded => i64::MAX,
        };

        let events: Vec<(i64, String, Vec<u8>)> = state
            .db
            .query(
                "select id, user, payload from events where id >= ? and id <= ?",
                [min, max],
            )
            .await?
            .parse_rows()
            .await?;

        Ok(events
            .into_iter()
            .map(|(idx, user, payload)| Event { idx, user, payload })
            .collect())
    }

    /// Import events directly into the stream without processing.
    ///
    /// This is a raw operation that isn't used in normal processing, but can be useful for
    /// restoring streams from backups, for example.
    pub async fn raw_import_events(&self, events: Vec<Event>) -> anyhow::Result<()> {
        let mut state = self.state.write().await;
        for event in events {
            if event.idx != state.latest_event + 1 {
                anyhow::bail!("Imported event not sequential");
            }
            state
                .db
                .execute(
                    "insert into events (id, user, payload) values (?, ?, ?)",
                    (event.idx, event.user, event.payload),
                )
                .await?;
            state.latest_event += 1;
        }
        Ok(())
    }

    /// Query from the stream.
    #[instrument(skip(self), err)]
    pub async fn query(&self, query: LeafQuery) -> Result<SqlRows, StreamError> {
        let state = self.state.read().await;
        let query_name = &query.query_name;
        let (module, module_db) = Self::ensure_module_loaded(&state.module_state)?;

        // Get the module's query definition by name
        let query_def = module
            .def()
            .queries
            .iter()
            .find(|x| x.name == query.query_name)
            .ok_or_else(|| StreamError::QueryDoesNotExistInModule(query_name.clone()))?;

        // Make sure the query is valid for it's definition
        query_def.validate_query(&query)?;

        // Execute the query
        module_db.authorizer(Some(Arc::new(module_query_authorizer)))?;
        let mut query_result = module_db
            .query(
                &module.def().materializer,
                query
                    .params
                    .into_iter()
                    .map(|(k, v)| (format!("${k}"), leaf_sql_value_to_libsql(v)))
                    .chain([
                        (
                            "$start".to_string(),
                            libsql::Value::Integer(query.start.unwrap_or(1)),
                        ),
                        (
                            "$limit".to_string(),
                            libsql::Value::Integer(query.limit.unwrap_or(100)),
                        ),
                    ])
                    .collect::<Vec<_>>(),
            )
            .await?;
        module_db.authorizer(None)?;

        // Convert the query result to our Leaf SqlRows type
        let column_count = query_result.column_count();
        let column_names = (0..column_count)
            .map(|i| query_result.column_name(i).unwrap_or("").to_string())
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        while let Some(row) = query_result.next().await? {
            rows.push(SqlRow {
                values: (0..column_count)
                    .map(|i| row.get_value(i).map(libsql_value_to_leaf))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }

        // Return the result
        Ok(SqlRows { rows, column_names })
    }

    /// This function returns a future that must be awaited on in order for subscriptions to the
    /// stream to be updated.
    ///
    /// The idea is that you will usually spawn this future as a background task on your executor of
    /// choice so that the stream can perform it's background tasks and keep the subscriptions to
    /// the stream up-to-date.
    ///
    /// If you call this function a second time it will return `None` because there may only be one
    /// worker task.
    pub async fn create_worker_task(&self) -> Option<impl Future<Output = ()> + 'static> {
        let this = self.clone();

        let mut state = self.state.write().await;
        if state.worker_sender.is_some() {
            return None;
        };

        let (sender, receiver) = async_channel::bounded(16);
        state.worker_sender = Some(sender.clone());
        drop(state);

        Some(async move {
            while let Ok(message) = receiver.recv().await {
                let (query_subscriptions, latest_event_subscriptions) = {
                    let state = this.state.read().await;
                    (
                        state.query_subscriptions.clone(),
                        state.latest_event_subscriptions.clone(),
                    )
                };
                let mut query_subscriptions = query_subscriptions.lock().await;
                let mut latest_event_subscriptions = latest_event_subscriptions.lock().await;

                let subs = match message {
                    WorkerMessage::NewEvents { latest_event } => {
                        latest_event_subscriptions.retain(|x| !x.is_closed());
                        for sub in &*latest_event_subscriptions {
                            sub.try_send(latest_event).ok();
                        }
                        query_subscriptions.iter_mut().collect::<Vec<_>>()
                    }
                    WorkerMessage::NeedsUpdate(id) => query_subscriptions
                        .iter_mut()
                        .find(|x| x.sub_id == id)
                        .into_iter()
                        .collect::<Vec<_>>(),
                };

                for sub in subs {
                    let query = sub.subscription_query.to_query(sub.latest_event);
                    let query_last_event = query.last_event();
                    let result = this.query(query).await;
                    sub.result_sender.try_send(result).unwrap();

                    let stream_latest_event = this.state.read().await.latest_event;
                    if query_last_event
                        .map(|l| l < stream_latest_event)
                        .unwrap_or(false)
                    {
                        sender.try_send(WorkerMessage::NeedsUpdate(sub.sub_id)).ok();
                    }
                    sub.latest_event = query_last_event.unwrap_or(stream_latest_event);
                }
            }
        })
    }
}

/// Install Leaf's user-defined functions on the database connection
fn install_udfs(db: &libsql::Connection) -> libsql::Result<()> {
    // A panic function that can be used to intentionally stop a transaction such as in the event
    // authorizer.
    db.create_scalar_function(ScalarFunctionDef {
        name: "panic".to_string(),
        num_args: -1,
        deterministic: true,
        innocuous: false,
        direct_only: true,
        callback: Arc::new(|values| {
            anyhow::bail!(
                "Panic from SQL: {}",
                values
                    .into_iter()
                    .map(|x| format!("{:?}", x))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }),
    })?;

    Ok(())
}

/// The initialization authorizer is the same as the materialize authorizer.
#[allow(non_upper_case_globals)]
const module_init_authorizer: fn(&libsql::AuthContext) -> libsql::Authorization =
    module_materialize_authorizer;

fn module_materialize_authorizer(ctx: &libsql::AuthContext) -> libsql::Authorization {
    use AuthAction::*;
    use Authorization::*;
    match (ctx.action, ctx.database_name) {
        (CreateIndex { .. }, None | Some("main") | Some("temp"))
        | (CreateTable { .. }, None | Some("main") | Some("temp"))
        | (CreateTempIndex { .. }, None | Some("main") | Some("temp"))
        | (CreateTempTable { .. }, None | Some("main") | Some("temp"))
        | (CreateTempTrigger { .. }, None | Some("main") | Some("temp"))
        | (CreateTempView { .. }, None | Some("main") | Some("temp"))
        | (CreateTrigger { .. }, None | Some("main") | Some("temp"))
        | (CreateView { .. }, None | Some("main") | Some("temp"))
        | (Delete { .. }, None | Some("main") | Some("temp"))
        | (DropIndex { .. }, None | Some("main") | Some("temp"))
        | (DropTable { .. }, None | Some("main") | Some("temp"))
        | (DropTempIndex { .. }, None | Some("main") | Some("temp"))
        | (DropTempTable { .. }, None | Some("main") | Some("temp"))
        | (DropTempTrigger { .. }, None | Some("main") | Some("temp"))
        | (DropTempView { .. }, None | Some("main") | Some("temp"))
        | (DropTrigger { .. }, None | Some("main") | Some("temp"))
        | (DropView { .. }, None | Some("main") | Some("temp"))
        | (Insert { .. }, None | Some("main") | Some("temp"))
        | (Read { .. }, None | Some("main") | Some("temp"))
        | (Select { .. }, None | Some("main") | Some("temp"))
        | (Update { .. }, None | Some("main") | Some("temp"))
        | (AlterTable { .. }, None | Some("main") | Some("temp"))
        | (Reindex { .. }, None | Some("main") | Some("temp"))
        | (Analyze { .. }, None | Some("main") | Some("temp"))
        | (Function { .. }, None | Some("main") | Some("temp"))
        | (Recursive { .. }, None | Some("main") | Some("temp"))
        | (Read { .. }, Some("events"))
        | (Select { .. }, Some("events")) => Allow,
        op => {
            tracing::warn!(
                ?op,
                "Denying SQL operation from default_module_db_authorizer"
            );
            Deny
        }
    }
}

#[allow(non_upper_case_globals)]
const module_authorize_authorizer: fn(&libsql::AuthContext) -> libsql::Authorization =
    read_only_module_db_authorizer;

#[allow(non_upper_case_globals)]
const module_query_authorizer: fn(&libsql::AuthContext) -> libsql::Authorization =
    read_only_module_db_authorizer;

fn read_only_module_db_authorizer(ctx: &libsql::AuthContext) -> libsql::Authorization {
    match ctx.action {
        libsql::AuthAction::Read { .. } | libsql::AuthAction::Select => {
            libsql::Authorization::Allow
        }
        op => {
            tracing::warn!(
                ?op,
                "Denying SQL operation from read_only_module_db_authorizer"
            );
            libsql::Authorization::Deny
        }
    }
}

impl std::hash::Hash for Stream {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.info.id.hash(state);
    }
}
impl std::cmp::Eq for Stream {}
impl std::cmp::PartialEq for Stream {
    fn eq(&self, other: &Self) -> bool {
        self.info.id == other.info.id
    }
}

fn leaf_sql_value_to_libsql(value: SqlValue) -> libsql::Value {
    use SqlValue as S;
    use libsql::Value as V;
    match value {
        S::Null => V::Null,
        S::Integer(i) => V::Integer(i),
        S::Real(r) => V::Real(r),
        S::Text(t) => V::Text(t),
        S::Blob(b) => V::Blob(b),
    }
}

fn libsql_value_to_leaf(value: libsql::Value) -> SqlValue {
    use SqlValue as S;
    use libsql::Value as V;
    match value {
        V::Null => S::Null,
        V::Integer(i) => S::Integer(i),
        V::Real(r) => S::Real(r),
        V::Text(t) => S::Text(t),
        V::Blob(b) => S::Blob(b),
    }
}

/// Check that the provided bytes represent a valid WASM file.
///
/// This doesn't check that the imports/exports of the module are compatible with Leaf, it just does
/// a very quick check that the bytes are valid according to the WASM binary file format.
pub fn validate_wasm(bytes: &[u8]) -> anyhow::Result<()> {
    wasmtime::Module::validate(&ENGINE, bytes)
}
