//! Unreads materializer module.
//!
//! This module provides the materialization system that processes events from all streams
//! to track unread message counts per user per room and space membership.

use std::{collections::HashMap, sync::Arc};

use async_channel::Sender;
use atproto_plc::Did;
use dasl::drisl::Value;
use leaf_stream::{
    Stream, drisl_extract::DrislExtractExprSegment, drisl_extract::extract_from_drisl_with_expr,
};
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::unreads::UnreadsDB;

use std::sync::LazyLock;

/// Global unreads materializer instance.
pub static UNREADS_MATERIALIZER: LazyLock<UnreadsMaterializer> =
    LazyLock::new(UnreadsMaterializer::default);

/// Unreads materializer that manages per-stream materialization.
pub struct UnreadsMaterializer {
    /// Active stream monitors keyed by stream DID
    monitors: Arc<RwLock<HashMap<Did, StreamMonitorHandle>>>,
    /// Semaphore to limit concurrent materialization tasks
    semaphore: Arc<Semaphore>,
}

impl Default for UnreadsMaterializer {
    fn default() -> Self {
        Self {
            monitors: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(100)), // Limit to 100 concurrent tasks
        }
    }
}

/// Handle to an active stream monitor.
struct StreamMonitorHandle {
    /// The stream DID
    stream_did: Did,
    /// The stream handle
    stream: Arc<Stream>,
    /// Join handle for the monitor task
    task_handle: JoinHandle<()>,
    /// Sender to signal the monitor to stop
    stop_tx: Sender<()>,
}

impl UnreadsMaterializer {
    /// Start monitoring a stream for unread tracking.
    #[instrument(skip(self, stream))]
    pub async fn start_monitoring(
        &self,
        stream_did: Did,
        stream: Arc<Stream>,
    ) -> anyhow::Result<()> {
        let mut monitors: tokio::sync::RwLockWriteGuard<'_, HashMap<Did, StreamMonitorHandle>> =
            self.monitors.write().await;

        // Check if already monitoring this stream
        if monitors.contains_key(&stream_did) {
            debug!("Stream {stream_did} is already being monitored");
            return Ok(());
        }

        // Get the data directory for this stream
        let data_dir = crate::storage::STORAGE.data_dir()?;
        let stream_dir = data_dir.join("streams").join(stream_did.as_str());

        // Initialize the unreads database for this stream
        let unreads_db = UnreadsDB::initialize(&stream_dir).await?;

        // Create event subscription
        let event_rx = stream.subscribe_events_stream().await;

        // Create stop channel
        let (stop_tx, stop_rx) = async_channel::bounded(1);

        // Spawn monitor task
        let stream_did_for_monitor = stream_did.clone();
        let stream_clone = stream.clone();
        let stream_did_for_log = stream_did.clone();
        let stream_did_for_error = stream_did.clone();
        let semaphore = self.semaphore.clone();
        let task_handle = tokio::spawn(async move {
            let result = run_stream_monitor(
                stream_did_for_monitor,
                stream_clone,
                unreads_db,
                event_rx,
                stop_rx,
                semaphore,
            )
            .await;

            if let Err(e) = result {
                error!("Stream monitor for stream {stream_did_for_error} failed: {e}");
            }
        });

        // Store monitor handle
        monitors.insert(
            stream_did.clone(),
            StreamMonitorHandle {
                stream_did,
                stream,
                task_handle,
                stop_tx,
            },
        );

        info!("Started monitoring stream {stream_did_for_log} for unread tracking");
        Ok(())
    }

    /// Stop monitoring a stream.
    #[instrument(skip(self))]
    pub async fn stop_monitoring(&self, stream_did: &Did) -> anyhow::Result<()> {
        let mut monitors: tokio::sync::RwLockWriteGuard<'_, HashMap<Did, StreamMonitorHandle>> =
            self.monitors.write().await;

        if let Some(handle) = monitors.remove(stream_did) {
            // Send stop signal
            let _ = handle.stop_tx.send(()).await;

            // Wait for task to finish (with timeout)
            let _ =
                tokio::time::timeout(tokio::time::Duration::from_secs(5), handle.task_handle).await;

            info!("Stopped monitoring stream {stream_did}");
        } else {
            debug!("Stream {stream_did} was not being monitored");
        }

        Ok(())
    }

    /// Get all stream DIDs currently being monitored.
    #[instrument(skip(self))]
    pub async fn get_monitored_streams(&self) -> Vec<Did> {
        let monitors = self.monitors.read().await;
        monitors.keys().cloned().collect()
    }

    /// Stop monitoring all streams.
    #[instrument(skip(self))]
    pub async fn stop_all(&self) {
        let monitors: tokio::sync::RwLockWriteGuard<'_, HashMap<Did, StreamMonitorHandle>> =
            self.monitors.write().await;
        let stream_dids: Vec<Did> = monitors.keys().cloned().collect();
        drop(monitors);

        for stream_did in stream_dids {
            if let Err(e) = self.stop_monitoring(&stream_did).await {
                error!("Error stopping monitor for {stream_did}: {e}");
            }
        }
    }
}

/// Run the stream monitor task.
#[instrument(skip(_stream, unreads_db, event_rx, stop_rx, semaphore))]
async fn run_stream_monitor(
    stream_did: Did,
    _stream: Arc<Stream>,
    unreads_db: UnreadsDB,
    event_rx: async_channel::Receiver<leaf_stream_types::Event>,
    stop_rx: async_channel::Receiver<()>,
    semaphore: Arc<Semaphore>,
) -> anyhow::Result<()> {
    // Get the last processed event index
    let state = unreads_db.get_materialization_state().await?;
    let mut last_processed_idx = state.last_event_idx;

    info!("Starting materialization for stream {stream_did} from event index {last_processed_idx}");

    // Process events until we receive a stop signal
    loop {
        tokio::select! {
            // Check for stop signal
            _ = stop_rx.recv() => {
                info!("Received stop signal for stream {stream_did}");
                break;
            }

            // Process next event
            event_result = event_rx.recv() => {
                let event = match event_result {
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

                // Acquire semaphore permit to limit concurrent processing
                let _permit = semaphore.acquire().await;

                // Process the event
                if let Err(e) = process_event(&stream_did, &event, &unreads_db).await {
                    error!("Error processing event {} for stream {stream_did}: {e}", event.idx);
                    // Continue processing other events even if one fails
                    continue;
                }

                // Update last processed index
                last_processed_idx = event.idx;

                // Update materialization state
                if let Err(e) = unreads_db.update_materialization_state(last_processed_idx).await {
                    error!("Error updating materialization state for stream {stream_did}: {e}");
                }
            }
        }
    }

    info!("Materialization stopped for stream {stream_did}");
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
            handle_join_space(event, &unreads_db).await?;
        }
        Some("LeaveSpace") | Some("leaveSpace") | Some("town.muni.event.LeaveSpace") => {
            handle_leave_space(event, &unreads_db).await?;
        }
        _ => {
            // For other events with a room ID, increment unreads for all members except sender
            if let Some(room_id) = room_id {
                handle_regular_event(event, &room_id, &unreads_db).await?;
            }
        }
    }

    Ok(())
}

/// Extract room ID from a DRISL payload.
fn extract_room_id(payload: &Value) -> Option<String> {
    // Try various paths where roomId might be located
    // Note: We can't use const arrays with String::from() in const context,
    // so we build the paths dynamically
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
    let increments: Vec<crate::unreads::UnreadIncrement> = other_members
        .iter()
        .map(|member| crate::unreads::UnreadIncrement {
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
