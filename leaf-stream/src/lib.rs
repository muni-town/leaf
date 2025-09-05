use std::{collections::HashMap, pin::Pin, sync::Arc};

use anyhow::Context;
use async_lock::{RwLock, RwLockUpgradableReadGuard};
use blake3::Hash;
use leaf_stream_types::{Decode, EventRequest, Inbound, IncomingEvent, Outbound, Process};
use leaf_utils::convert::*;
use libsql::Connection;
use parity_scale_codec::Encode;
use tracing::instrument;
use ulid::Ulid;

pub use async_broadcast::Receiver;
pub type EventReceiver = async_broadcast::Receiver<Event>;

pub mod encoding;
use encoding::Encodable;

pub use blake3;
pub use leaf_stream_types as types;
pub use libsql;
pub use ulid;

use crate::modules::wasm::ENGINE;

pub mod modules;

/// The genesis configuration of an event stream.
#[derive(Encode, Decode, Debug)]
pub struct StreamGenesis {
    /// A ULID, which encompasses the timestamp and additional randomness, included in this stream
    /// to make it's hash unique.
    ///
    /// Note that this is not the stream ID, which is computed fro the hash of the
    /// [`GenesisStreamConfig`].
    pub stamp: Encodable<Ulid>,
    /// User ID of the user that created the stream.
    pub creator: String,
    /// The hash of the WASM module that will be used for filtering.
    pub module: Encodable<Hash>,
    /// The parameters to supply to the WASM module.
    pub params: Vec<u8>,
}

impl StreamGenesis {
    /// Compute the stream ID of this stream based on it's genesis config.
    pub fn get_stream_id_and_bytes(&self) -> (Hash, Vec<u8>) {
        let encoded = self.encode();
        (blake3::hash(&encoded), encoded)
    }
}

#[derive(Debug)]
pub struct Stream {
    id: Hash,
    db: libsql::Connection,
    module_state: Arc<RwLock<ModuleState>>,
    creator: String,
    latest_event: i64,
    module_event_cursor: i64,
    /// This is an event that needs to be sent to subscribers but we are waiting until a new module
    /// is provided to do the filtering on the event.
    pending_event_for_subscribers: Option<Event>,
    subscribers: Arc<RwLock<HashMap<String, async_broadcast::Sender<Event>>>>,
    worker_sender: Option<async_channel::Sender<Event>>,
}

#[derive(Debug)]
struct ModuleState {
    load: ModuleLoad,
    params: Vec<u8>,
}

enum ModuleLoad {
    Unloaded(Hash),
    Loaded {
        module: Arc<dyn LeafModule>,
        module_db: libsql::Connection,
    },
}

impl std::fmt::Debug for ModuleLoad {
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
}

impl Stream {
    pub fn id(&self) -> blake3::Hash {
        self.id
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
        let params;
        let module_event_cursor;
        if let Some(row) = row {
            let (db_stream_id, db_module, db_params, db_module_event_cursor): (
                Hash,
                Hash,
                Vec<u8>,
                Option<i64>,
            ) = row
                .parse_row()
                .await
                .context("error parsing stream state")?;
            if db_stream_id != id {
                return Err(StreamError::IdMismatch {
                    expected_id: id,
                    database_id: db_stream_id,
                });
            }
            module = ModuleLoad::Unloaded(db_module);
            params = db_params;
            module_event_cursor = db_module_event_cursor.unwrap_or(0);
        } else {
            // Initialize the stream state
            db.execute(
                "insert into stream_state \
                (id, creator, stream_id, module, params, module_event_cursor) values \
                (1, :creator, :stream_id, :module, :params, null) ",
                (
                    (":stream_id", id.as_bytes().to_vec()),
                    (":creator", genesis.creator.clone()),
                    (":module", genesis.module.0.as_bytes().to_vec()),
                    (":params", genesis.params.clone()),
                ),
            )
            .await
            .context("error initializing stream state")?;

            module = ModuleLoad::Unloaded(genesis.module.0);
            params = genesis.params;
            module_event_cursor = 0;
        };

        let subscribers = Default::default();

        Ok(Self {
            id,
            db,
            module_state: Arc::new(RwLock::new(ModuleState {
                load: module,
                params,
            })),
            subscribers,
            creator: genesis.creator,
            module_event_cursor,
            latest_event,
            worker_sender: None,
            pending_event_for_subscribers: None,
        })
    }

    pub async fn subscribe(&self, requesting_user: &str) -> EventReceiver {
        let mut subs = self.subscribers.write().await;

        // Take the opportunity to clean up any closed subscriptions
        subs.retain(|_k, v| !v.is_closed());

        // Return a new receiver for an existing subscription for the user, or create a new channel.
        match subs.get(requesting_user) {
            Some(sender) => sender.new_receiver(),
            None => {
                let (sender, receiver) = async_broadcast::broadcast(12);
                subs.insert(requesting_user.into(), sender);
                receiver
            }
        }
    }

    /// If this stream needs a Leaf module to be loaded before it can continue processing events,
    /// then this will return `Some(hash)`.
    ///
    /// You must then call [`provide_module()`][Self::provide_module] with the module in order to
    /// allow the stream to continue processing.
    pub async fn needs_module(&self) -> Option<blake3::Hash> {
        match self.module_state.read().await.load {
            ModuleLoad::Unloaded(hash) => Some(hash),
            _ => None,
        }
    }

    /// Provide the stream it's module, if it is needed.
    pub async fn provide_module(
        &mut self,
        module: Arc<dyn LeafModule>,
        module_db: libsql::Connection,
    ) -> Result<(), StreamError> {
        let mut module_state = self.module_state.write().await;
        match &module_state.load {
            ModuleLoad::Unloaded(hash) => {
                if module.id() == *hash {
                    module_state.load = ModuleLoad::Loaded { module, module_db };

                    // Send our pending event if we had an event that we were waiting to send to
                    // subscribers because we didn't have a module.
                    if let Some(event) = self.pending_event_for_subscribers.take()
                        && let Some(sender) = &self.worker_sender
                    {
                        sender.try_send(event).ok();
                    }
                    Ok(())
                } else {
                    Err(StreamError::InvalidModuleId {
                        needed: *hash,
                        provided: module.id(),
                    })
                }
            }
            ModuleLoad::Loaded { .. } => Err(StreamError::ModuleNotNeeded),
        }
    }

    fn ensure_module_loaded(
        module: &ModuleLoad,
    ) -> Result<(&dyn LeafModule, &Connection), StreamError> {
        match module {
            ModuleLoad::Unloaded(hash) => Err(StreamError::ModuleNotProvided(*hash)),
            ModuleLoad::Loaded { module, module_db } => Ok((module.as_ref(), module_db)),
        }
    }

    /// Make sure that the module has processed all of the events in the stream so far.
    ///
    /// Returns the number of events that were processed while catching up.
    #[instrument(skip(self), err)]
    pub async fn catch_up_module(&mut self) -> Result<i64, StreamError> {
        let module_state = self.module_state.read().await;
        let (module, module_db) = Self::ensure_module_loaded(&module_state.load)?;

        // TODO: should we make sure this only ever runs once or just have them make sure the SQL is
        // idempodent like we do now? ( Right now there are edge cases where we might call this
        // twice. )
        if self.module_event_cursor == 0 {
            module
                .init_db(
                    self.creator.clone(),
                    module_state.params.clone(),
                    module_db.clone(),
                )
                .await
                .context("Error initializing module DB")?;
        }

        assert!(
            self.latest_event >= self.module_event_cursor,
            "Somehow the module event cursor is higher than the latest event."
        );
        let events_behind = self.latest_event - self.module_event_cursor;
        if events_behind == 0 {
            return Ok(events_behind);
        }

        let events: Vec<(i64, String, Vec<u8>)> = self
            .db
            .query(
                "select id, user, payload from events where id > ?",
                [self.module_event_cursor],
            )
            .await?
            .parse_rows()
            .await?;

        for (id, user, payload) in events {
            assert_eq!(id, self.module_event_cursor + 1);

            // NOTE: we **ignore** module updates while we are catching up modules. Module updates
            // are only allowed to happen on new events.
            let _module_update = module
                .process_event(
                    IncomingEvent {
                        payload,
                        params: module_state.params.clone(),
                        user,
                    },
                    module_db.clone(),
                )
                .await?;

            self.module_event_cursor += 1;
            self.db
                .execute(
                    "update stream_state set module_event_cursor = ? where id = 1",
                    [self.module_event_cursor],
                )
                .await?;
        }

        assert_eq!(
            self.latest_event, self.module_event_cursor,
            "Module event cursor still not caught up."
        );

        Ok(events_behind)
    }

    /// Handle an event, adding it to the stream if it is valid.
    ///
    /// Returns `Some(hash)` if handling this event triggered a module change for the stream. The
    /// hash is the ID of the new module. You will need to load that module and call
    /// [`provide_module()`] and preferably [`catch_up_module()`] before calling `handle_event()`
    /// again.
    ///
    /// Calling [`catch_up_module()`] is optional will cause the stream to replay all the events
    /// through the new module and may take longer than handling a normal event. Running it in the
    /// background would be a good idea if possible.
    ///
    /// If you don't call `catch_up_module()` it will automatically be called the next time you call
    /// `handle_vent()`, but again, it will run much slower than handling a normal event because of
    /// the need to replay.
    #[instrument(skip(self, payload), err)]
    pub async fn handle_event(
        &mut self,
        user: String,
        payload: Vec<u8>,
    ) -> Result<Option<Hash>, StreamError> {
        // Make sure the current module is caught up
        self.catch_up_module()
            .await
            .context("error catching up module")?;

        let module_state = self.module_state.upgradable_read().await;
        let (module, module_db) = Self::ensure_module_loaded(&module_state.load)?;

        // For filter operations, switch the SQL authorization to only allow read operations.
        module_db.authorizer(Some(Arc::new(read_only_sql_authorizer)))?;

        // First we check whether the event should be filtered out
        let filter_response = module
            .filter_inbound(
                IncomingEvent {
                    payload: payload.clone(),
                    params: module_state.params.clone(),
                    user: user.clone(),
                },
                module_db.clone(),
            )
            .await
            .context("error running module filter_inbound.")?;

        // Now disable the read-only authorizer
        module_db.authorizer(None)?;

        // Error if the event was rejected by the filter
        if let Inbound::Block { reason } = filter_response {
            return Err(StreamError::EventRejected { reason });
        }

        // Now that the event has passed the filter, add it to the stream.
        self.latest_event += 1;
        // TODO: add SQL trigger to enforce incrementing the ID once in the database
        self.db
            .execute(
                "insert into events (id, user, payload) values (:id, :user, :payload)",
                (
                    (":id", self.latest_event),
                    (":user", user.clone()),
                    (":payload", payload.clone()),
                ),
            )
            .await?;

        // TODO: We need to think more carefully around this portion of the code what happens if we
        // crash at different timings and if that causes the same event to be processed twice or
        // something weird like that.
        //
        // This is probably just something that needs to be handled in the SQL of the module, making
        // sure that it tracks the fact that it has handled an event atomically with any
        // modifications it makes to the database because of that event.

        // Now that the event has been included in the stream, we can let the module process it.
        let module_update = module
            .process_event(
                IncomingEvent {
                    payload: payload.clone(),
                    params: module_state.params.clone(),
                    user: user.clone(),
                },
                module_db.clone(),
            )
            .await?;

        // Now we update the module's event cursor
        self.module_event_cursor += 1;
        assert_eq!(self.module_event_cursor, self.latest_event);
        self.db
            .execute(
                "update stream_state set module_event_cursor = ? where id = 1",
                [self.module_event_cursor],
            )
            .await
            .context("error updating module event cursor")?;

        if module_update.new_module.is_some() || module_update.new_params.is_some() {
            let mut module_state = RwLockUpgradableReadGuard::upgrade(module_state).await;

            // If there is a new module, then update our module status
            if let Some(new_module) = module_update.new_module {
                self.db
                    .execute(
                        "update stream_state set module = ? where id = 1",
                        [new_module.to_vec()],
                    )
                    .await?;
                module_state.load = ModuleLoad::Unloaded(Hash::from_bytes(new_module));
            }
            // If there are new params, then update our params
            if let Some(new_params) = module_update.new_params {
                self.db
                    .execute("update stream_state set params = ?", [new_params.clone()])
                    .await?;
                module_state.params = new_params;
            }
        }

        let event = Event {
            idx: self.latest_event,
            user,
            payload,
        };

        // If there is not a new module, then we can send a notification to our subscribers
        // immediately.
        if module_update.new_module.is_none() {
            if let Some(sender) = &self.worker_sender {
                sender.try_send(event).ok();
            }

        // If there is a new module, we need to store it for later and it will get sent when our new
        // module is provided.
        } else {
            self.pending_event_for_subscribers = Some(event);
        }

        Ok(module_update.new_module.map(Hash::from_bytes))
    }

    /// TODO: provide a way to asynchronously stream the events instead of batch collecting them.
    pub async fn fetch_events(
        &self,
        requesting_user: &str,
        offset: u64,
        limit: u64,
    ) -> Result<Vec<Event>, StreamError> {
        let module_state = self.module_state.upgradable_read().await;
        let (module, module_db) = Self::ensure_module_loaded(&module_state.load)?;

        let mut filtered = Vec::with_capacity(limit as usize);

        let mut page = 0;
        while filtered.len() < limit as usize {
            let rows = self
                .db
                .query(
                    "select id, user, payload from events where id >= :start and id < :end",
                    (
                        (":start", offset + page * limit),
                        (":end", offset + (page + 1) * limit),
                    ),
                )
                .await?;
            page += 1;
            let events: Vec<(i64, String, Vec<u8>)> = rows.parse_rows().await?;

            if events.is_empty() {
                break;
            }

            module_db.authorizer(Some(Arc::new(read_only_sql_authorizer)))?;
            for (idx, user, payload) in events {
                let result = module
                    .filter_outbound(
                        EventRequest {
                            requesting_user: requesting_user.into(),
                            incoming_event: IncomingEvent {
                                payload: payload.clone(),
                                params: module_state.params.clone(),
                                user: user.clone(),
                            },
                        },
                        module_db.clone(),
                    )
                    .await?;
                match result {
                    Outbound::Allow => filtered.push(Event { idx, user, payload }),
                    Outbound::Block => (),
                }
            }
            module_db.authorizer(None)?;
        }

        Ok(filtered)
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
    pub fn creat_worker_task(&mut self) -> Option<impl Future<Output = ()> + 'static> {
        let module_state_lock = self.module_state.clone();
        let subs = self.subscribers.clone();
        if self.worker_sender.is_some() {
            return None;
        };

        let (sender, receiver) = async_channel::bounded(16);
        self.worker_sender = Some(sender);

        let id = self.id;
        Some(async move {
            while let Ok(event) = receiver.recv().await {
                let subscribers = subs.read().await;
                let module_state = module_state_lock.read().await;
                let Ok((module, module_db)) = Self::ensure_module_loaded(&module_state.load) else {
                    // This shouldn't be able to happen, because we try to only send events here when the module is loaded.
                    tracing::error!(
                        "Failed to update notification because the module was not loaded."
                    );
                    continue;
                };

                if let Err(e) = module_db.authorizer(Some(Arc::new(read_only_sql_authorizer))) {
                    tracing::warn!("Error setting SQLite authorizer to read only: {e}");
                }
                let notification_futures = subscribers.iter().map(|(requesting_user, sender)| {
                    let requesting_user = requesting_user.clone();
                    async {
                        let outbound = module
                            .filter_outbound(
                                EventRequest {
                                    requesting_user,
                                    incoming_event: IncomingEvent {
                                        payload: event.payload.clone(),
                                        params: module_state.params.clone(),
                                        user: event.user.clone(),
                                    },
                                },
                                module_db.clone(),
                            )
                            .await;
                        match outbound {
                            Ok(o) => match o {
                                Outbound::Allow => {
                                    sender.broadcast(event.clone()).await.ok();
                                }
                                Outbound::Block => (),
                            },
                            Err(e) => tracing::warn!(
                                "Error in outbound filter for stream {id} for event {}: {e}",
                                event.idx
                            ),
                        }
                    }
                });
                futures::future::join_all(notification_futures).await;
                if let Err(e) = module_db.authorizer(None) {
                    tracing::warn!("Error setting SQLite authorizer to None: {e}");
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub idx: i64,
    pub user: String,
    pub payload: Vec<u8>,
}

/// The trait implemented by leaf modules.
pub trait LeafModule: Sync + Send {
    fn id(&self) -> blake3::Hash;
    fn init_db(
        &self,
        creator: String,
        params: Vec<u8>,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
    fn filter_inbound(
        &self,
        moduel_input: IncomingEvent,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Inbound>> + Send>>;
    fn filter_outbound(
        &self,
        moduel_input: EventRequest,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Outbound>> + Send>>;
    fn process_event(
        &self,
        moduel_input: IncomingEvent,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<Process>> + Send>>;
}

fn read_only_sql_authorizer(ctx: &libsql::AuthContext) -> libsql::Authorization {
    match ctx.action {
        libsql::AuthAction::Read { .. } | libsql::AuthAction::Select => {
            libsql::Authorization::Allow
        }
        _ => libsql::Authorization::Deny,
    }
}

/// Check that the provided bytes represent a valid WASM file.
///
/// This doesn't check that the imports/exports of the module are compatible with Leaf, it just does
/// a very quick check that the bytes are valid according to the WASM binary file format.
pub fn validate_wasm(bytes: &[u8]) -> anyhow::Result<()> {
    wasmtime::Module::validate(&ENGINE, bytes)
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
