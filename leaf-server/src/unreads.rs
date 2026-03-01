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

    // ============================================================================
    // space_members table operations
    // ============================================================================

    /// Add a member to the space
    #[instrument(skip(self), err)]
    pub async fn add_member(&self, user_did: &str, _event_idx: i64) -> anyhow::Result<()> {
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
    pub async fn remove_member(&self, user_did: &str, _event_idx: i64) -> anyhow::Result<()> {
        self.db()
            .execute("delete from space_members where user_did = ?", [user_did])
            .await?;
        Ok(())
    }

    /// Get all active members of the space
    #[instrument(skip(self), err)]
    pub async fn get_space_members(&self) -> anyhow::Result<Vec<SpaceMember>> {
        let rows: Vec<String> = self
            .db()
            .query("select user_did from space_members", ())
            .await?
            .parse_rows()
            .await?;

        Ok(rows
            .into_iter()
            .map(|user_did| SpaceMember { user_did })
            .collect())
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

    // ============================================================================
    // room_unreads table operations
    // ============================================================================

    /// Get unreads for a user across all rooms
    #[instrument(skip(self), err)]
    pub async fn get_user_unreads(&self, user_did: &str) -> anyhow::Result<Vec<RoomUnread>> {
        let rows: Vec<(String, i64, i64, Option<i64>, i64)> = self
            .db()
            .query(
                "select room_id, unread_count, mention_count, last_event_idx, updated_at from room_unreads where user_did = ? order by updated_at desc",
                [user_did],
            )
            .await?
            .parse_rows()
            .await?;

        Ok(rows
            .into_iter()
            .map(
                |(room_id, unread_count, mention_count, last_event_idx, updated_at)| RoomUnread {
                    room_id,
                    unread_count,
                    mention_count,
                    last_event_idx,
                    updated_at,
                },
            )
            .collect())
    }

    /// Get unreads for a user in a specific room
    #[instrument(skip(self), err)]
    pub async fn get_user_unreads_for_room(
        &self,
        user_did: &str,
        room_id: &str,
    ) -> anyhow::Result<Option<RoomUnread>> {
        let mut rows = self
            .db()
            .query(
                "select room_id, unread_count, mention_count, last_event_idx, updated_at from room_unreads where user_did = ? and room_id = ?",
                (user_did, room_id),
            )
            .await?;

        if let Some(row) = rows.next().await? {
            let (room_id, unread_count, mention_count, last_event_idx, updated_at): (
                String,
                i64,
                i64,
                Option<i64>,
                i64,
            ) = row.parse_row().await?;
            return Ok(Some(RoomUnread {
                room_id,
                unread_count,
                mention_count,
                last_event_idx,
                updated_at,
            }));
        }

        Ok(None)
    }

    /// Increment unread counts for multiple users
    #[instrument(skip(self, increments), err)]
    pub async fn increment_unreads(&self, increments: Vec<UnreadIncrement>) -> anyhow::Result<()> {
        let db = self.db();
        let trans = db.transaction().await?;

        for inc in increments {
            trans
                .execute(
                    "insert into room_unreads (user_did, room_id, unread_count, mention_count, last_event_idx, updated_at)
                     values (?, ?, ?, ?, ?, unixepoch())
                     on conflict (user_did, room_id) do update set
                        unread_count = unread_count + ?,
                        mention_count = mention_count + ?,
                        last_event_idx = ?,
                        updated_at = unixepoch()",
                    (
                        inc.user_did.as_str(),
                        inc.room_id.as_str(),
                        inc.unread_delta,
                        inc.mention_delta,
                        inc.event_idx,
                        inc.unread_delta,
                        inc.mention_delta,
                        inc.event_idx,
                    ),
                )
                .await?;
        }

        trans.commit().await?;
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
                "update room_unreads set unread_count = 0, mention_count = 0, last_event_idx = max(last_event_idx, ?), updated_at = unixepoch() where user_did = ? and room_id = ?",
                (last_read_idx, user_did, room_id),
            )
            .await?;
        Ok(())
    }

    /// Reset all unread counts for a user
    #[instrument(skip(self), err)]
    pub async fn reset_user_unreads(&self, user_did: &str) -> anyhow::Result<()> {
        self.db()
            .execute(
                "update room_unreads set unread_count = 0, mention_count = 0, updated_at = unixepoch() where user_did = ?",
                [user_did],
            )
            .await?;
        Ok(())
    }

    // ============================================================================
    // materialization_state table operations
    // ============================================================================

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
        self.db()
            .execute(
                "insert into materialization_state (last_event_idx) values (?)
                 on conflict do update set last_event_idx = ?",
                (last_event_idx, last_event_idx),
            )
            .await?;
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

/// Represents a space member
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SpaceMember {
    /// The DID of the user
    pub user_did: String,
}

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
    /// Timestamp of last update
    pub updated_at: i64,
}

/// Increment operation for unreads
#[derive(Debug, Clone)]
pub struct UnreadIncrement {
    /// The DID of the user
    pub user_did: String,
    /// The room ID
    pub room_id: String,
    /// Delta for unread count
    pub unread_delta: i64,
    /// Delta for mention count
    pub mention_delta: i64,
    /// The event index
    pub event_idx: i64,
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

    info!("Starting unreads monitor for stream {stream_did} from event index {last_processed_idx}");

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
            // Return Ok to skip this event without stopping the monitor
            return Ok(());
        }
    };

    // Extract room ID from payload
    let room_id = extract_room_id(&payload);

    // Extract event type (discriminant)
    let event_type = extract_event_type(&payload);

    debug!(
        "Processing event {} in stream {stream_did}: room_id={:?}, event_type={:?}",
        event.idx, room_id, event_type
    );

    // Handle different event types
    match event_type.as_deref() {
        Some("JoinSpace") | Some("joinSpace") | Some("town.muni.event.JoinSpace") => {
            handle_join_space(event, unreads_db).await?;
        }
        Some("LeaveSpace") | Some("leaveSpace") | Some("town.muni.event.LeaveSpace") => {
            handle_leave_space(event, unreads_db).await?;
        }
        _ => {
            // For other events with a room ID, increment unreads for all members except sender
            if let Some(room_id) = room_id {
                handle_regular_event(event, &room_id, unreads_db).await?;
            }
        }
    }

    Ok(())
}

/// Extract room ID from a DRISL payload.
fn extract_room_id(payload: &Value) -> Option<String> {
    // Try various paths where roomId might be located
    let paths: Vec<Vec<DrislExtractExprSegment>> = vec![
        vec![DrislExtractExprSegment::FieldAccess("roomId".to_string())],
        vec![DrislExtractExprSegment::FieldAccess("room_id".to_string())],
        vec![
            DrislExtractExprSegment::FieldAccess("message".to_string()),
            DrislExtractExprSegment::FieldAccess("roomId".to_string()),
        ],
        vec![
            DrislExtractExprSegment::FieldAccess("message".to_string()),
            DrislExtractExprSegment::FieldAccess("room_id".to_string()),
        ],
        vec![
            DrislExtractExprSegment::FieldAccess("post".to_string()),
            DrislExtractExprSegment::FieldAccess("roomId".to_string()),
        ],
        vec![
            DrislExtractExprSegment::FieldAccess("post".to_string()),
            DrislExtractExprSegment::FieldAccess("room_id".to_string()),
        ],
    ];

    for path in &paths {
        if let Some(Value::Text(room_id)) = extract_from_drisl_with_expr(payload.clone(), path) {
            return Some(room_id);
        }
    }

    None
}

/// Extract event type (discriminant) from a DRISL payload.
fn extract_event_type(payload: &Value) -> Option<String> {
    match payload {
        Value::Map(map) => {
            // If the map has only one key, it's likely a tagged union discriminant
            if map.len() == 1 {
                return Some(map.keys().next().unwrap().clone());
            }
            // Try to extract from a $type field
            if let Some(Value::Text(type_str)) = map.get("$type") {
                return Some(type_str.clone());
            }
            None
        }
        Value::Text(text) => Some(text.clone()),
        _ => None,
    }
}

/// Handle a JoinSpace event.
#[instrument(skip(event, unreads_db))]
async fn handle_join_space(
    event: &leaf_stream_types::Event,
    unreads_db: &UnreadsDB,
) -> anyhow::Result<()> {
    // Add the user as a member of the space
    unreads_db.add_member(&event.user, event.idx).await?;

    debug!(
        "Added member {} to space at event index {}",
        event.user, event.idx
    );

    Ok(())
}

/// Handle a LeaveSpace event.
#[instrument(skip(event, unreads_db))]
async fn handle_leave_space(
    event: &leaf_stream_types::Event,
    unreads_db: &UnreadsDB,
) -> anyhow::Result<()> {
    // Remove the user from the space
    unreads_db.remove_member(&event.user, event.idx).await?;

    // Clean up unread records for this user
    unreads_db.reset_user_unreads(&event.user).await?;

    debug!(
        "Removed member {} from space at event index {} and cleaned up unreads",
        event.user, event.idx
    );

    Ok(())
}

/// Handle a regular event (not JoinSpace/LeaveSpace) with a room ID.
#[instrument(skip(event, unreads_db))]
async fn handle_regular_event(
    event: &leaf_stream_types::Event,
    room_id: &str,
    unreads_db: &UnreadsDB,
) -> anyhow::Result<()> {
    // Get all active members of the space
    let members = unreads_db.get_space_members().await?;

    // Filter out the sender
    let other_members: Vec<_> = members
        .into_iter()
        .filter(|member| member.user_did != event.user)
        .collect();

    if other_members.is_empty() {
        debug!("No other members to notify for room {room_id}");
        return Ok(());
    }

    // Create increment operations for all other members
    let increments: Vec<UnreadIncrement> = other_members
        .iter()
        .map(|member| UnreadIncrement {
            user_did: member.user_did.clone(),
            room_id: room_id.to_string(),
            unread_delta: 1,
            mention_delta: 0, // TODO: Extract mentions from payload
            event_idx: event.idx,
        })
        .collect();

    // Increment unreads for all members
    unreads_db.increment_unreads(increments).await?;

    debug!(
        "Incremented unreads for {} members in room {room_id} at event index {}",
        other_members.len(),
        event.idx
    );

    Ok(())
}
