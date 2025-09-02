use std::{pin::Pin, sync::Arc};

use blake3::Hash;
use leaf_utils::convert::*;
use parity_scale_codec::Encode;
use tracing::instrument;
use ulid::Ulid;

mod encoding;
use encoding::Encodable;

pub mod modules;

pub struct Stream {
    pub id: Hash,
    pub db: libsql::Connection,
    pub module_db: libsql::Connection,
    pub module: ModuleStatus,
    pub params: Vec<u8>,
    pub latest_event: i64,
    pub module_event_cursor: i64,
}

/// The genesis configuration of an event stream.
#[derive(Encode, Debug)]
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

pub enum ModuleStatus {
    Unloaded(Hash),
    Loaded(Box<dyn LeafModule>),
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
    /// Open a stream.
    pub async fn open(
        genesis: StreamGenesis,
        db: libsql::Connection,
        module_db: libsql::Connection,
    ) -> Result<Self, StreamError> {
        let id = genesis.get_stream_id_and_bytes().0;

        // run database migrations
        db.execute_batch(include_str!("./streamdb_schema_00.sql"))
            .await?;

        // Get the latest event index from the database
        let latest_event = db
            .query("select max(id) from events;", ())
            .await?
            .next()
            .await?;
        let latest_event = if let Some(row) = latest_event {
            i64::from_row(row).await?
        } else {
            0
        };

        // Load the stream state from the database
        let row = db
            .query(
                "select (stream_id, module, params, module_event_cursor) \
                from stream_state where id=1",
                (),
            )
            .await?
            .next()
            .await?;

        // Parse the current state from the database or initialize a new state if one does not
        // exist.
        let module;
        let params;
        let module_event_cursor;
        if let Some(row) = row {
            let (db_stream_id, db_module, db_params, db_module_event_cursor) =
                row.parse_row().await?;
            if db_stream_id != id {
                return Err(StreamError::IdMismatch {
                    expected_id: id,
                    database_id: db_stream_id,
                });
            }

            module = ModuleStatus::Unloaded(db_module);
            params = db_params;
            module_event_cursor = db_module_event_cursor;
        } else {
            // Initialize the stream state
            db.query(
                "insert into stream_state \
                (id, stream_id, module, params, module_event_cursor) values \
                (1, :stream_id, :module, :params, 0) ",
                (
                    (":stream_id", id.as_bytes().to_vec()),
                    (":module", genesis.module.0.as_bytes().to_vec()),
                    (":params", genesis.params.clone()),
                ),
            )
            .await?;

            module = ModuleStatus::Unloaded(genesis.module.0);
            params = genesis.params;
            module_event_cursor = 0;
        };

        Ok(Self {
            id,
            db,
            module_db,
            module,
            params,
            module_event_cursor,
            latest_event,
        })
    }

    /// If this stream needs a Leaf module to be loaded before it can continue processing events,
    /// then this will return `Some(hash)`.
    ///
    /// You must then call [`provide_module()`][Self::provide_module] with the module in order to
    /// allow the stream to continue processing.
    fn needs_module(&self) -> Option<blake3::Hash> {
        match self.module {
            ModuleStatus::Unloaded(hash) => Some(hash),
            _ => None,
        }
    }

    /// Provide the stream it's module, if it is needed.
    pub fn provide_module(&mut self, module: Box<dyn LeafModule>) -> Result<(), StreamError> {
        let Some(needed) = self.needs_module() else {
            return Err(StreamError::ModuleNotNeeded);
        };
        let provided = module.id();
        if provided != needed {
            return Err(StreamError::InvalidModuleId { needed, provided });
        }

        self.module = ModuleStatus::Loaded(module);

        Ok(())
    }

    /// Make sure that the module has processed all of the events in the stream so far.
    ///
    /// Returns the number of events that were processed while catching up.
    #[instrument(skip(self), err)]
    pub async fn catch_up_module(&mut self) -> Result<i64, StreamError> {
        let ModuleStatus::Loaded(module) = &mut self.module else {
            return Err(StreamError::ModuleNotNeeded);
        };

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
                "select (id, user, payload) from events where id > :cursor",
                (":cursor", self.module_event_cursor),
            )
            .await?
            .parse_rows()
            .await?;

        for (id, user, payload) in events {
            assert_eq!(id, self.module_event_cursor + 1);

            // NOTE: we **ignore** module updates while we are catching up modules. Module updates
            // are only allowed to happen on new events.
            let _module_update = module
                .process_event(payload, self.params.clone(), user, self.module_db.clone())
                .await?;

            self.db
                .query(
                    "update stream_state set module_event_cursor = :cursor where id = 1",
                    (":cursor", self.module_event_cursor + 1),
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
        self.catch_up_module().await?;

        let module = match &mut self.module {
            ModuleStatus::Unloaded(hash) => return Err(StreamError::ModuleNotProvided(*hash)),
            ModuleStatus::Loaded(module) => module,
        };

        // For filter operations, switch the SQL authorization to only allow read operations.
        self.module_db
            .authorizer(Some(Arc::new(read_only_sql_authorizer)))?;

        // First we check whether the event should be filtered out
        let filter_response = module
            .filter_inbound(
                payload.clone(),
                self.params.clone(),
                user.clone(),
                self.module_db.clone(),
            )
            .await?;

        // Now disable the read-only authorizer
        self.module_db.authorizer(None)?;

        // Error if the event was rejected by the filter
        if let InboundFilterResponse::Reject { reason } = filter_response {
            return Err(StreamError::EventRejected { reason });
        }

        // Now that the event has passed the filter, add it to the stream.
        self.db
            .query(
                "insert into events (user, payload) values (:user, :payload)",
                ((":user", user.clone()), (":payload", payload.clone())),
            )
            .await?;
        self.latest_event += 1;

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
                payload.clone(),
                self.params.clone(),
                user,
                self.module_db.clone(),
            )
            .await?;

        // Now we update the module's event cursor
        self.module_event_cursor += 1;
        self.db
            .query(
                "update stream_state set module_event_cursor = :cursor where id = 1",
                (":cursor", self.module_event_cursor),
            )
            .await?;

        // If there is a new module, then update our module status
        if let Some(new_module) = module_update.new_module {
            self.db
                .execute(
                    "update stream_state set module = :module where id = 1",
                    (":module", new_module.as_bytes().to_vec()),
                )
                .await?;
            self.module = ModuleStatus::Unloaded(new_module);
        }
        // If there are new params, then update our params
        if let Some(new_params) = module_update.new_params {
            self.db
                .execute(
                    "update stream_state set params = :params",
                    (":params", new_params.clone()),
                )
                .await?;
            self.params = new_params;
        }

        Ok(module_update.new_module)
    }
}

#[derive(Debug, Clone)]
pub enum InboundFilterResponse {
    Accept,
    Reject { reason: String },
}

/// The trait implemented by leaf modules.
pub trait LeafModule {
    fn id(&self) -> blake3::Hash;
    fn filter_inbound(
        &mut self,
        payload: Vec<u8>,
        params: Vec<u8>,
        user: String,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<InboundFilterResponse>>>>;
    fn filter_outbound(
        &mut self,
        payload: Vec<u8>,
        params: Vec<u8>,
        user: String,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<bool>>>>;
    fn process_event(
        &mut self,
        payload: Vec<u8>,
        params: Vec<u8>,
        user: String,
        db: libsql::Connection,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<ModuleUpdate>>>>;
}

/// An update that should be made to the Leaf module for a stream.
#[derive(Default)]
pub struct ModuleUpdate {
    new_module: Option<Hash>,
    new_params: Option<Vec<u8>>,
}

fn read_only_sql_authorizer(ctx: &libsql::AuthContext) -> libsql::Authorization {
    match ctx.action {
        libsql::AuthAction::Read { .. } | libsql::AuthAction::Select => {
            libsql::Authorization::Allow
        }
        _ => libsql::Authorization::Deny,
    }
}
