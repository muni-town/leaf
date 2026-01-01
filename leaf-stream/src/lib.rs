use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use anyhow::Context;
use async_lock::{Mutex, RwLock};
use atproto_plc::{Did, SigningKey};
use dasl::cid::{Cid, Codec::Drisl as DrislCodec};
use leaf_stream_types::{
    Event, IncomingEvent, LeafQuery, LeafSubscribeEventsResponse, QueryValidationError, SqlRows,
};
use leaf_utils::convert::*;
use libsql::{AuthAction, Authorization, Connection};
use tracing::instrument;
use ulid::Ulid;

pub type SubscriptionResultReceiver =
    async_channel::Receiver<Result<LeafSubscribeEventsResponse, StreamError>>;
pub type SubscriptionResultSender =
    async_channel::Sender<Result<LeafSubscribeEventsResponse, StreamError>>;

pub use module::*;
mod module;

mod drisl_extract;

pub use atproto_plc;
pub use dasl;
pub use leaf_stream_types as types;
pub use libsql;
pub use ulid;

#[derive(Debug, Clone)]
pub struct Stream {
    id: Did,
    state: Arc<RwLock<StreamState>>,
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
    pub user: Option<String>,
    pub subscription_query: LeafQuery,
    pub latest_event: i64,
    pub result_sender: SubscriptionResultSender,
}

enum ModuleState {
    /// The module has not been loaded yet. If there is an inner [`Cid`] it means that the stream
    /// has a module configured for it that has not been loaded yet. If it is [`None`] then it means
    /// that there is no module selected for this stream yet.
    Unloaded(Option<Cid>),
    // A module has been loaded.
    Loaded {
        module: Arc<dyn LeafModule>,
        module_db: libsql::Connection,
    },
}
impl ModuleState {
    fn id(&self) -> Option<Cid> {
        match self {
            ModuleState::Unloaded(id) => *id,
            ModuleState::Loaded { module, .. } => Some(module.id()),
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
    #[error("Query does not contain any SQL statements")]
    EmptyQuery,
    #[error(
        "The opened database is for a stream with a different ID. \
        The expected ID was {expected_id} but the ID in the database was {database_id}."
    )]
    IdMismatch { expected_id: Did, database_id: Did },
    #[error("Attempted to provide module for stream when it was not needed.")]
    ModuleNotNeeded,
    #[error("The stream's module has not been provided. Expecte module ID: {0:?}")]
    ModuleNotProvided(Option<Cid>),
    #[error("The stream module hash could not be decoded from the database")]
    ModuleHashDecodeError,
    #[error(
        "Attempted to provide module with different ID than the one needed \
        by the stream. Needed {needed} but got {provided}"
    )]
    InvalidModuleId { needed: Cid, provided: Cid },
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
    #[error("Error validating query: {0}")]
    QueryValidationError(#[from] QueryValidationError),
}

impl Stream {
    pub fn id(&self) -> &Did {
        &self.id
    }

    pub async fn latest_event(&self) -> i64 {
        self.state.read().await.latest_event
    }

    pub async fn module_cid(&self) -> Option<Cid> {
        self.state.read().await.module_state.id()
    }

    /// This is a raw method to set the current module of the stream, without any other processing.
    /// You usually should not use this, but you may need it if you are, for example, importing the
    /// stream from a backup.
    pub async fn raw_set_module(&self, module_cid: Cid) -> anyhow::Result<()> {
        let mut state = self.state.write().await;
        state
            .db
            .execute(
                "update stream_state set module_cid = ?, module_event_cursor = null where id = 1",
                [module_cid.as_bytes().to_vec()],
            )
            .await?;
        state.module_event_cursor = 0;
        state.module_state = ModuleState::Unloaded(Some(module_cid));

        Ok(())
    }

    /// Open a stream.
    #[instrument(skip(db))]
    pub async fn open(id: Did, db: libsql::Connection) -> Result<Self, StreamError> {
        // run database migrations
        db.execute_batch(include_str!("./streamdb_schema_00.sql"))
            .await?;

        // Get the latest event index from the database
        let latest_event = db
            .query("select max(idx) from events", ())
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
                "select stream_did, module_cid, module_event_cursor \
                from stream_state where id=1",
                (),
            )
            .await?
            .next()
            .await?;

        // Parse the current state from the database or initialize a new state if one does not
        // exist.
        let module_state;
        let module_event_cursor;
        if let Some(row) = row {
            let (db_stream_did, module_cid, db_module_event_cursor): (
                Did,
                Option<Cid>,
                Option<i64>,
            ) = row.parse_row().await?;
            if db_stream_did != id {
                return Err(StreamError::IdMismatch {
                    expected_id: id,
                    database_id: db_stream_did,
                });
            }
            module_state = ModuleState::Unloaded(module_cid);
            module_event_cursor = db_module_event_cursor.unwrap_or(0);
        } else {
            // Initialize the stream state
            db.execute(
                "insert into stream_state \
                (id, stream_did, module_cid, module_event_cursor) values \
                (1, ?, null, null) ",
                [id.as_str().to_string()],
            )
            .await?;

            module_state = ModuleState::Unloaded(None);
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
            id,
            state: Arc::new(RwLock::new(StreamState {
                db_filename,
                db,
                module_state,
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

    /// Subscribe to events returned by a query.
    pub async fn subscribe_events(
        &self,
        user: Option<String>,
        subscription_query: LeafQuery,
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
            user,
            latest_event: subscription_query
                .start
                .map(|s| s - 1)
                .unwrap_or(state.latest_event),
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
    /// then this will return `Some(x)`. If the stream has a specific module it needs, then `x` will
    /// be `Some(module_cid)`.
    ///
    /// If this is a new stream that hasn't had a module before then `x` will be `None`.
    ///
    /// You must then call [`provide_module()`][Self::provide_module] with the module in order to
    /// allow the stream to continue processing.
    pub async fn needs_module(&self) -> Option<Option<Cid>> {
        let state = self.state.read().await;
        match &state.module_state {
            ModuleState::Unloaded(id) => Some(*id),
            _ => None,
        }
    }

    /// Provide the stream it's module, if it is needed.
    pub async fn provide_module(
        &self,
        module: Arc<dyn LeafModule>,
        module_db: libsql::Connection,
    ) -> Result<(), StreamError> {
        let mut state = self.state.write().await;
        match &state.module_state {
            ModuleState::Unloaded(needed_id) => {
                if needed_id.map(|id| id == module.id()).unwrap_or(true) {
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

                    // Allow the module to install UDFs.
                    module.init_db_conn(&module_db).await?;

                    state.module_state = ModuleState::Loaded { module, module_db };
                    drop(state);

                    // Make sure we catch up the new module if it needs it.
                    self.raw_catch_up_module().await?;

                    Ok(())
                } else {
                    Err(StreamError::InvalidModuleId {
                        needed: needed_id.unwrap_or_else(|| Cid::empty_sha2_256(DrislCodec)),
                        provided: module.id(),
                    })
                }
            }
            ModuleState::Loaded { .. } => Err(StreamError::ModuleNotNeeded),
        }
    }

    fn ensure_module_loaded(
        module: &ModuleState,
    ) -> Result<(Arc<dyn LeafModule>, &Connection), StreamError> {
        match module {
            ModuleState::Unloaded(module_cid) => Err(StreamError::ModuleNotProvided(*module_cid)),
            ModuleState::Loaded { module, module_db } => Ok((module.clone(), module_db)),
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
            let init_result = module.init_db_schema(module_db).await;
            module_db.authorizer(None)?;
            init_result?;
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
        let events: Vec<(i64, String, Vec<u8>, Vec<u8>)> = state
            .db
            .query(
                "select idx, user, payload, signature from events where idx > ?",
                [state.module_event_cursor],
            )
            .await?
            .parse_rows()
            .await?;
        let even_count = events.len();

        // Start a new transaction
        module_db.execute("begin immediate", ()).await?;

        let result = async {
            for (i, (idx, user, payload, signature)) in events.into_iter().enumerate() {
                assert_eq!(idx, state.module_event_cursor + 1 + i as i64);

                // Execute the materializer for the event
                module_db.authorizer(Some(Arc::new(module_materialize_authorizer)))?;
                let result = module
                    .materialize(
                        module_db,
                        Event {
                            idx,
                            user,
                            payload,
                            signature,
                        },
                    )
                    .await;
                module_db.authorizer(None)?;
                result?;

                // Increment the event cursor
                module_db
                    .execute(
                        "update events.stream_state set module_event_cursor = ? where id = 1",
                        [state.module_event_cursor + 1 + i as i64],
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
    #[instrument(skip(self, events, signing_key), err)]
    pub async fn add_events(
        &self,
        signing_key: SigningKey,
        events: Vec<IncomingEvent>,
    ) -> Result<Option<Cid>, StreamError> {
        // Make sure the current module is caught up
        self.raw_catch_up_module()
            .await
            .context("error catching up module")?;

        let mut state = self.state.write().await;
        let event_count = events.len();

        // NOTE: we use a write lock here because we are starting a transaction and need to make
        // sure other async tasks don't come and try to use this same database connection while the
        // transaction is in progress.
        let (module, module_db) = Self::ensure_module_loaded(&state.module_state)?;

        // Start a new transaction
        tracing::debug!("Starting new transaction");
        module_db.execute("begin immediate", ()).await?;

        let result = async {
            // TODO: we should probably have a separate mechanism for storing the batch signature
            // once, and we should also include the event indexes in the signature.

            // Sign the whole event batch. It is much slower to sign each event individually.
            let signature = signing_key.sign(&dasl::drisl::to_vec(&events).unwrap())?;

            for event in events {
                // Execute the authorizer
                tracing::debug!("Running authorizer");

                module_db.authorizer(Some(Arc::new(module_authorize_authorizer)))?;
                let result = module.authorize(module_db, event.clone()).await;
                module_db.authorizer(None)?;
                result?;

                tracing::debug!("Authorized");


                // Insert the event into the events table
                let idx = module_db
                    .query(
                        r#"insert into events.events
                        select null as idx, user, payload, ? as signature from event returning idx"#,
                        [signature.clone()],
                    )
                    .await?
                    .next()
                    .await?
                    .ok_or_else(|| anyhow::format_err!("Internal error getting event index"))?
                    .get::<i64>(0)?;

                tracing::debug!("Inserted event into events table");

                // Execute the materializer for the event
                module_db.authorizer(Some(Arc::new(module_materialize_authorizer)))?;
                let result = module
                    .materialize(
                        module_db,
                        Event {
                            idx,
                            user: event.user,
                            payload: event.payload,
                            signature: signature.clone(),
                        },
                    )
                    .await;
                module_db.authorizer(None)?;
                result?;

                tracing::debug!("Materialized event");

                // Increment the module event cursor
                module_db
                    .execute(
                        r#"
                            update events.stream_state
                            set module_event_cursor = ?
                            where id = 1
                        "#,
                        [idx],
                    )
                    .await?;
            }

            anyhow::Ok(())
        }
        .await;

        // Handle errors by rolling back the transaction
        if let Err(e) = result {
            module_db.authorizer(None)?;
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

        let events: Vec<(i64, String, Vec<u8>, Vec<u8>)> = state
            .db
            .query(
                "select idx, user, payload, signature from events where idx >= ? and idx <= ?",
                [min, max],
            )
            .await?
            .parse_rows()
            .await?;

        Ok(events
            .into_iter()
            .map(|(idx, user, payload, signature)| Event {
                idx,
                user,
                payload,
                signature,
            })
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
            // TODO: Add signatures to the database
            state
                .db
                .execute(
                    "insert into events (id, user, payload, signature) values (?, ?, ?, ?)",
                    (event.idx, event.user, event.payload),
                )
                .await?;
            state.latest_event += 1;
        }
        Ok(())
    }

    /// Query from the stream.
    #[instrument(skip(self), err)]
    pub async fn query(
        &self,
        user: Option<String>,
        query: LeafQuery,
    ) -> Result<SqlRows, StreamError> {
        let state = self.state.read().await;
        let (module, module_db) = Self::ensure_module_loaded(&state.module_state)?;

        // Start a new read transaction
        module_db.execute("begin deferred", ()).await?;

        module_db.authorizer(Some(Arc::new(module_query_authorizer)))?;
        let result = module.query(module_db, user, query).await;
        module_db.authorizer(None)?;
        let result = match result {
            Ok(result) => result,
            Err(e) => {
                module_db.execute("rollback", ()).await?;
                return Err(e.into());
            }
        };

        // Finish the read transaction
        module_db.execute("commit", ()).await?;

        // Return the result
        Ok(result)
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

                // Clean up any closed subscriptions
                query_subscriptions.retain(|x| !x.result_sender.is_closed());
                latest_event_subscriptions.retain(|x| !x.is_closed());

                let subs = match message {
                    WorkerMessage::NewEvents { latest_event } => {
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

                let stream_latest_event = this.state.read().await.latest_event;
                for sub in subs {
                    let query = sub
                        .subscription_query
                        .update_for_subscription(sub.latest_event + 1);
                    let query_last_event = query.last_event().min(stream_latest_event);

                    let query_limit = query.limit;
                    let result = this.query(sub.user.clone(), query).await.map(|rows| {
                        LeafSubscribeEventsResponse {
                            has_more: rows.len() as i64 >= query_limit
                                && query_last_event < stream_latest_event,
                            rows,
                        }
                    });

                    let has_more = result.as_ref().map(|x| x.has_more).unwrap_or(false);

                    sub.result_sender.try_send(result).ok();

                    if has_more {
                        sender.try_send(WorkerMessage::NeedsUpdate(sub.sub_id)).ok();
                    }
                    sub.latest_event = query_last_event;
                }
            }
        })
    }
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
    match (ctx.action, ctx.database_name) {
        (_op, Some("temp")) => libsql::Authorization::Allow,
        (libsql::AuthAction::Read { .. } | libsql::AuthAction::Select, _db) => {
            libsql::Authorization::Allow
        }
        // TODO: this is kind of specific to the module which isn't ideal. We may need to move the
        // authorization responsibility to the module.
        (libsql::AuthAction::Function { function_name }, _db) => match function_name {
            "unauthorized" | "throw" | "coalesce" | "->>" | "->" | "drisl_extract" => {
                libsql::Authorization::Allow
            }
            _ => libsql::Authorization::Deny,
        },
        (op, db) => {
            tracing::warn!(
                ?op,
                ?db,
                "Denying SQL operation from read_only_module_db_authorizer"
            );
            libsql::Authorization::Deny
        }
    }
}

impl std::hash::Hash for Stream {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl std::cmp::Eq for Stream {}
impl std::cmp::PartialEq for Stream {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
