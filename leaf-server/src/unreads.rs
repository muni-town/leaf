//! Unreads tracking database module.
//!
//! This module provides database infrastructure for tracking unread message counts
//! per user per room and space membership on a per-stream basis.

use std::{path::Path, sync::Arc};

use atproto_plc::Did;
use dasl::drisl::Value;
use leaf_stream::drisl_extract::{DrislExtractExprSegment, extract_from_drisl_with_expr};
use leaf_utils::convert::{ParseRow, ParseRows};
use libsql::Connection;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::streams::StreamWithUnreads;

/// Global SQLite PRAGMA settings for WAL mode and performance
pub static GLOBAL_SQLITE_PRAGMA: &str = "pragma synchronous = normal; pragma journal_mode = wal;";

/// Unreads database manager for a single stream
///
/// Manages the unreads tracking database stored at `{data_dir}/streams/{stream_did}/unreads.db`.
pub struct UnreadsDB {
    /// Database connection
    db: Connection,
}

impl UnreadsDB {
    /// Initialize the unreads database for a stream
    ///
    /// Opens the database file at `{stream_dir}/unreads.db`, applies WAL mode settings,
    /// and runs the schema migrations.
    #[instrument(err)]
    pub async fn initialize(stream_dir: &Path) -> anyhow::Result<Self> {
        // Create the stream directory if it doesn't exist
        tokio::fs::create_dir_all(stream_dir).await?;

        // Open the database file
        let database = libsql::Builder::new_local(stream_dir.join("unreads.db"))
            .build()
            .await?;
        let c = database.connect()?;
        c.execute_batch(GLOBAL_SQLITE_PRAGMA).await?;
        tracing::info!(
            "Unreads database connected at {}",
            stream_dir.join("unreads.db").display()
        );

        // Run migrations
        run_database_migrations(&c).await?;

        Ok(UnreadsDB { db: c })
    }

    /// Get the database connection
    fn db(&self) -> &Connection {
        &self.db
    }

    /// Add a member to the space
    #[instrument(skip(self), err)]
    pub async fn add_member(&self, user_did: &str) -> anyhow::Result<()> {
        self.db()
            .execute(
                "insert into space_members (user_did) values (?)",
                [user_did],
            )
            .await?;
        Ok(())
    }

    /// Remove a member from the space
    #[instrument(skip(self), err)]
    pub async fn remove_member(&self, user_did: &str) -> anyhow::Result<()> {
        self.db()
            .execute("delete from space_members where user_did = ?", [user_did])
            .await?;
        Ok(())
    }

    /// Check if a user is an active member of the space
    #[instrument(skip(self), err)]
    pub async fn is_member(&self, user_did: &str) -> anyhow::Result<bool> {
        let mut rows = self
            .db()
            .query("select 1 from space_members where user_did = ?", [user_did])
            .await?;
        Ok(rows.next().await?.is_some())
    }

    /// Get unreads for a user across all rooms
    #[instrument(skip(self), err)]
    pub async fn get_user_unreads(&self, user_did: &str) -> anyhow::Result<Vec<RoomUnread>> {
        let rows: Vec<(String, i64, i64, Option<i64>)> = self
            .db()
            .query(
                "select room_id, unread_count, mention_count, last_event_idx from room_unreads where user_did = ?",
                [user_did],
            )
            .await?
            .parse_rows()
            .await?;

        Ok(rows
            .into_iter()
            .map(
                |(room_id, unread_count, mention_count, last_event_idx)| RoomUnread {
                    room_id,
                    unread_count,
                    mention_count,
                    last_event_idx,
                },
            )
            .collect())
    }

    /// Increment unreads for all space members except the sender in a single SQL operation.
    #[instrument(skip(self), err)]
    pub async fn increment_unreads_for_all_members_except(
        &self,
        room_id: &str,
        exclude_user_did: &str,
        event_idx: i64,
    ) -> anyhow::Result<()> {
        self.db()
            .execute(
                "insert into room_unreads (user_did, room_id, unread_count, mention_count, last_event_idx)
                 select user_did, ?, 1, 0, ? from space_members where user_did != ?
                 on conflict (user_did, room_id) do update set
                    unread_count = unread_count + 1,
                    mention_count = mention_count + 0,
                    last_event_idx = ?",
                (room_id, event_idx, exclude_user_did, event_idx),
            )
            .await?;
        Ok(())
    }

    /// Mark items as read for a user in a specific room
    #[instrument(skip(self), err)]
    pub async fn mark_as_read(
        &self,
        user_did: &str,
        room_id: &str,
        last_read_idx: i64,
    ) -> anyhow::Result<()> {
        // First check if user is a member
        if !self.is_member(user_did).await? {
            anyhow::bail!("User {user_did} is not a member of this space");
        }

        self.db()
            .execute(
                "update room_unreads set unread_count = 0, mention_count = 0, last_event_idx = max(last_event_idx, ?) where user_did = ? and room_id = ?",
                (last_read_idx, user_did, room_id),
            )
            .await?;
        Ok(())
    }

    /// Reset all unread counts for a user
    #[instrument(skip(self), err)]
    pub async fn reset_user_unreads(&self, user_did: &str) -> anyhow::Result<()> {
        self.db()
            .execute("delete from room_unreads where user_did = ?", [user_did])
            .await?;
        Ok(())
    }

    /// Get the materialization state
    #[instrument(skip(self), err)]
    pub async fn get_materialization_state(&self) -> anyhow::Result<MaterializationState> {
        let mut rows = self
            .db()
            .query("select last_event_idx from materialization_state", ())
            .await?;

        if let Some(row) = rows.next().await? {
            let last_event_idx: i64 = row.parse_row().await?;
            return Ok(MaterializationState { last_event_idx });
        }

        // Return default state if not found
        Ok(MaterializationState { last_event_idx: 0 })
    }

    /// Update the materialization state
    #[instrument(skip(self), err)]
    pub async fn update_materialization_state(&self, last_event_idx: i64) -> anyhow::Result<()> {
        let trans = self.db().transaction().await?;
        trans
            .execute("delete from materialization_state", ())
            .await?;
        trans
            .execute(
                "insert into materialization_state (last_event_idx) values (?)",
                [last_event_idx],
            )
            .await?;
        trans.commit().await?;
        Ok(())
    }
}

/// Run database migrations
#[instrument(skip(db))]
async fn run_database_migrations(db: &Connection) -> anyhow::Result<()> {
    db.execute_transactional_batch(include_str!("unreads_schema.sql"))
        .await?;
    Ok(())
}

// ============================================================================
// Data types
// ============================================================================

/// Represents unread counts for a room
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoomUnread {
    /// The room ID
    pub room_id: String,
    /// Count of unread messages
    pub unread_count: i64,
    /// Count of mentions
    pub mention_count: i64,
    /// The last event index that was processed
    pub last_event_idx: Option<i64>,
}

/// Represents the materialization state for the stream
#[derive(Debug, Clone)]
pub struct MaterializationState {
    /// The last event index that was materialized
    pub last_event_idx: i64,
}

// ============================================================================
// Unreads Monitor
// ============================================================================

/// Run the unreads monitor task for a stream.
///
/// This function subscribes to stream events and processes them to track unread counts.
/// It automatically exits when the stream events channel is closed.
///
/// # Arguments
/// * `stream_with_unreads` - The stream with its associated unreads database
///
/// # Returns
/// A JoinHandle for the monitor task
#[instrument(skip(stream_with_unreads))]
pub fn run_unreads_monitor(stream_with_unreads: Arc<StreamWithUnreads>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let stream_did = stream_with_unreads.stream.id().clone();
        let result = monitor_stream(stream_with_unreads).await;

        if let Err(e) = result {
            error!("Unreads monitor for stream {stream_did} failed: {e}");
        }
    })
}

/// Monitor a stream and process events for unreads tracking.
#[instrument(skip(stream_with_unreads))]
async fn monitor_stream(stream_with_unreads: Arc<StreamWithUnreads>) -> anyhow::Result<()> {
    let stream_did = stream_with_unreads.stream.id().clone();

    // Subscribe to stream events
    let event_rx = stream_with_unreads.stream.subscribe_events_stream().await;

    // Get the last processed event index
    let state = stream_with_unreads
        .unreads_db
        .get_materialization_state()
        .await?;
    let mut last_processed_idx = state.last_event_idx;

    // Process events until the channel is closed
    loop {
        let event = match event_rx.recv().await {
            Ok(event) => event,
            Err(_) => {
                // Channel closed, exit loop
                debug!("Event channel closed for stream {stream_did}");
                break;
            }
        };

        // Skip events we've already processed
        if event.idx <= last_processed_idx {
            continue;
        }

        // Process the event
        if let Err(e) = process_event(&stream_did, &event, &stream_with_unreads.unreads_db).await {
            error!(
                "Error processing event {} for stream {stream_did}: {e}",
                event.idx
            );
            // Continue processing other events even if one fails
            continue;
        }

        // Update last processed index
        last_processed_idx = event.idx;

        // Update materialization state
        if let Err(e) = stream_with_unreads
            .unreads_db
            .update_materialization_state(last_processed_idx)
            .await
        {
            error!("Error updating materialization state for stream {stream_did}: {e}");
        }
    }

    info!("Unreads monitor stopped for stream {stream_did}");
    Ok(())
}

/// Process a single event.
#[instrument(skip(event, unreads_db))]
async fn process_event(
    stream_did: &Did,
    event: &leaf_stream_types::Event,
    unreads_db: &UnreadsDB,
) -> anyhow::Result<()> {
    // Parse DRISL payload
    let payload = match dasl::drisl::from_slice::<Value>(&event.payload) {
        Ok(value) => value,
        Err(e) => {
            warn!(
                "Failed to parse DRISL payload for event {} in stream {stream_did}: {e}",
                event.idx
            );
            return Ok(());
        }
    };

    // Extract event type (discriminant)
    let event_type = extract_from_drisl_with_expr(
        payload.clone(),
        &[DrislExtractExprSegment::FieldAccess("$type".to_string())],
    );

    // Extract room ID from payload
    let room_id = extract_from_drisl_with_expr(
        payload.clone(),
        &[DrislExtractExprSegment::FieldAccess("room".to_string())],
    );

    debug!(
        "Processing event {} in stream {stream_did}: room_id={:?}, event_type={:?}",
        event.idx, room_id, event_type
    );

    // Handle different event types
    match event_type {
        Some(Value::Text(event_type)) if event_type == "space.roomy.space.joinSpace.v0" => {
            unreads_db.add_member(&event.user).await?;
        }
        Some(Value::Text(event_type)) if event_type == "space.roomy.space.leaveSpace.v0" => {
            unreads_db.remove_member(&event.user).await?;
        }
        _ => {
            // For other events with a room ID, increment unreads for all members except sender
            if let Some(Value::Text(room_id)) = room_id {
                unreads_db
                    .increment_unreads_for_all_members_except(&room_id, &event.user, event.idx)
                    .await?;
            }
        }
    }

    Ok(())
}
