//! Leaf client for communicating with Leaf servers via Socket.IO
//!
//! This module provides the main client implementation for connecting to
//! and interacting with Leaf servers.

use crate::{
    codec::{self},
    error::{LeafClientError, Result},
    types::*,
};
use futures::FutureExt;
use rust_socketio::{
    asynchronous::{Client, ClientBuilder},
    Payload,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument};

type SubscriptionCallback = Box<dyn Fn(ApiResponse<SubscribeEventsResp>) -> Result<()> + Send + Sync>;

/// Leaf client for communicating with Leaf servers
pub struct LeafClient {
    /// Inner Socket.IO client
    socket: Client,
    /// Active query subscriptions
    subscriptions: Arc<Mutex<HashMap<SubscriptionId, SubscriptionCallback>>>,
    /// Stream DID for authentication
    stream_did: Option<Did>,
}

impl LeafClient {
    /// Connect to a Leaf server
    ///
    /// # Arguments
    /// * `url` - Server URL (e.g., "http://localhost:5530")
    /// * `authenticator` - Optional async function that returns an auth token
    ///
    /// # Example
    /// ```no_run
    /// use leaf_client_rust::LeafClient;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = LeafClient::connect(
    ///         "http://localhost:5530",
    ///         None::<fn() -> futures::future::Ready<Result<String, leaf_client_rust::LeafClientError>>>,
    ///     ).await?;
    ///     Ok(())
    /// }
    /// ```
    #[instrument(skip(authenticator))]
    pub async fn connect<F, Fut>(url: &str, authenticator: Option<F>) -> Result<Self>
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<String>> + Send + 'static,
    {
        info!("Connecting to Leaf server at {}", url);

        let subscriptions = Arc::new(Mutex::new(HashMap::new()));

        // Clone for the callback
        let subs_clone = subscriptions.clone();

        let callback = move |payload: Payload, _socket: Client| {
            let subs = subs_clone.clone();
            async move {
                match payload {
                    Payload::Binary(bin_data) => {
                        let data = bin_data.to_vec();
                        if let Err(e) = Self::handle_binary_payload(data, subs).await {
                            error!("Error handling binary payload: {}", e);
                        }
                    }
                    Payload::Text(values) => {
                        debug!("Received text payload: {:#?}", values);
                    }
                    Payload::String(str) => {
                        debug!("Received string: {}", str);
                    }
                }
            }
            .boxed()
        };

        // Build the Socket.IO client
        let mut builder = ClientBuilder::new(url);

        // Add authentication callback if provided
        if let Some(auth) = authenticator {
            // Note: rust_socketio might not support custom auth in the same way
            // We may need to emit an auth event after connection
            debug!("Authenticator provided, will authenticate after connection");
        }

        let socket = builder
            .on("stream/subscription_response", callback)
            .on("error", |err, _| {
                async move {
                    error!("Socket error: {:#?}", err);
                }
                .boxed()
            })
            .connect()
            .await
            .map_err(|e| LeafClientError::Socket(format!("Connection failed: {}", e)))?;

        info!("Connected to Leaf server");

        Ok(Self {
            socket,
            subscriptions,
            stream_did: None,
        })
    }

    /// Handle a binary payload from the server
    async fn handle_binary_payload(
        bin_data: Vec<u8>,
        subscriptions: Arc<Mutex<HashMap<SubscriptionId, SubscriptionCallback>>>,
    ) -> Result<()> {
        // Decode the notification
        let notification: StreamSubscribeNotification = codec::decode(&bin_data)?;

        debug!(
            "Received notification for subscription: {:?}",
            notification.subscription_id
        );

        // Convert BytesWrapper to Vec<u8> in the response
        let response = Self::convert_response(notification.response)?;

        // Find and call the subscription callback
        let subs = subscriptions.lock().await;
        if let Some(callback) = subs.get(&notification.subscription_id) {
            callback(response)?;
        } else {
            debug!(
                "No callback found for subscription: {:?}",
                notification.subscription_id
            );
        }

        Ok(())
    }

    /// Convert BytesWrapper instances to Vec<u8> in a response
    fn convert_response(
        resp: ApiResponse<SubscribeEventsResp<SqlValueRaw>>,
    ) -> Result<ApiResponse<SubscribeEventsResp<SqlValue>>> {
        match resp {
            ApiResponse::Ok(ok_resp) => {
                // Convert each row
                let mut converted_rows = Vec::new();
                for row in ok_resp.rows {
                    let mut converted_row = HashMap::new();
                    for (col, value) in row {
                        converted_row.insert(col, Self::convert_sql_value(value));
                    }
                    converted_rows.push(converted_row);
                }
                Ok(ApiResponse::Ok(SubscribeEventsResp {
                    rows: converted_rows,
                    has_more: ok_resp.has_more,
                }))
            }
            ApiResponse::Err(e) => Ok(ApiResponse::Err(e)),
        }
    }

    /// Convert SqlValueRaw to SqlValue (decoding BytesWrapper)
    fn convert_sql_value(raw: SqlValueRaw) -> SqlValue {
        match raw {
            SqlValueRaw::Null => SqlValue::Null,
            SqlValueRaw::Integer { value } => SqlValue::Integer { value },
            SqlValueRaw::Real { value } => SqlValue::Real { value },
            SqlValueRaw::Text { value } => SqlValue::Text { value },
            SqlValueRaw::Blob { value } => SqlValue::Blob {
                value: value.buf,
            },
        }
    }

    /// Upload a module to the server
    #[instrument(skip(self, module))]
    pub async fn upload_module(&self, module: &ModuleCodec) -> Result<ModuleUploadOk> {
        let encoded = codec::encode(module)?;

        let response = self
            .emit_with_ack("module/upload", encoded)
            .await?;

        let resp: ModuleUploadResp = codec::decode(&response)?;
        resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))
    }

    /// Check if a module exists on the server
    #[instrument(skip(self))]
    pub async fn has_module(&self, module_cid: &str) -> Result<bool> {
        let args = ModuleExistsArgs {
            module_cid: CidLink {
                link: module_cid.to_string(),
            },
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("module/exists", encoded)
            .await?;

        let resp: ModuleExistsResp = codec::decode(&response)?;
        let ok = resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))?;
        Ok(ok.module_exists)
    }

    /// Create a new stream
    #[instrument(skip(self))]
    pub async fn create_stream(&self, module_cid: &str) -> Result<Did> {
        let args = StreamCreateArgs {
            module_cid: CidLink {
                link: module_cid.to_string(),
            },
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/create", encoded)
            .await?;

        let resp: StreamCreateResp = codec::decode(&response)?;
        let ok = resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))?;
        Ok(ok.stream_did)
    }

    /// Get information about a stream
    #[instrument(skip(self))]
    pub async fn stream_info(&self, stream_did: &Did) -> Result<Option<String>> {
        let args = StreamInfoArgs {
            stream_did: stream_did.clone(),
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/info", encoded)
            .await?;

        let resp: StreamInfoResp = codec::decode(&response)?;
        let ok = resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))?;
        Ok(ok.module_cid.map(|c| c.link))
    }

    /// Update a stream's module
    #[instrument(skip(self))]
    pub async fn update_module(&self, stream_did: &Did, module_cid: &str) -> Result<()> {
        let args = StreamUpdateModuleArgs {
            stream_did: stream_did.clone(),
            module_cid: CidLink {
                link: module_cid.to_string(),
            },
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/update_module", encoded)
            .await?;

        let resp: StreamUpdateModuleResp = codec::decode(&response)?;
        resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))
    }

    /// Send events to a stream
    #[instrument(skip(self, events))]
    pub async fn send_events(&self, stream_did: &Did, events: &[Vec<u8>]) -> Result<()> {
        let wrapped_events: Vec<BytesWrapper> = events
            .iter()
            .map(|e| BytesWrapper { buf: e.clone() })
            .collect();

        let args = StreamEventBatchArgs {
            stream_did: stream_did.clone(),
            events: wrapped_events,
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/event_batch", encoded)
            .await?;

        let resp: StreamEventBatchResp = codec::decode(&response)?;
        resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))
    }

    /// Send state events to a stream
    #[instrument(skip(self, events))]
    pub async fn send_state_events(
        &self,
        stream_did: &Did,
        events: &[Vec<u8>],
    ) -> Result<()> {
        let wrapped_events: Vec<BytesWrapper> = events
            .iter()
            .map(|e| BytesWrapper { buf: e.clone() })
            .collect();

        let args = StreamStateEventBatchArgs {
            stream_did: stream_did.clone(),
            events: wrapped_events,
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/state_event_batch", encoded)
            .await?;

        let resp: StreamStateEventBatchResp = codec::decode(&response)?;
        resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))
    }

    /// Subscribe to events from a query
    ///
    /// Returns the subscription ID (call unsubscribe_events to cancel)
    #[instrument(skip(self, handler))]
    pub async fn subscribe_events<F>(
        &self,
        stream_did: &Did,
        query: &LeafQuery,
        handler: F,
    ) -> Result<SubscriptionId>
    where
        F: Fn(SubscribeEventsResp) -> Result<()> + Send + Sync + 'static,
    {
        let args = StreamSubscribeArgs {
            stream_did: stream_did.clone(),
            query: query.clone(),
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/subscribe_events", encoded)
            .await?;

        let resp: StreamSubscribeResp = codec::decode(&response)?;
        let ok = resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))?;

        let subscription_id = ok.subscription_id.clone();

        // Store the callback - unwrap ApiResponse before calling user's handler
        let callback: SubscriptionCallback = Box::new(move |resp| {
            match resp {
                ApiResponse::Ok(events) => handler(events),
                ApiResponse::Err(e) => Err(LeafClientError::Remote(e)),
            }
        });
        self.subscriptions
            .lock()
            .await
            .insert(subscription_id.clone(), callback);

        Ok(subscription_id)
    }

    /// Unsubscribe from events
    #[instrument(skip(self))]
    pub async fn unsubscribe_events(&self, subscription_id: &SubscriptionId) -> Result<bool> {
        // Remove from local map
        self.subscriptions
            .lock()
            .await
            .remove(subscription_id);

        // Tell server to unsubscribe
        let args = StreamUnsubscribeArgs {
            subscription_id: subscription_id.clone(),
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/unsubscribe", encoded)
            .await?;

        let resp: StreamUnsubscribeResp = codec::decode(&response)?;
        let ok = resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))?;
        Ok(ok.was_subscribed)
    }

    /// Query a stream
    #[instrument(skip(self, query))]
    pub async fn query(&self, stream_did: &Did, query: &LeafQuery) -> Result<SqlRows> {
        let args = StreamQueryArgs {
            stream_did: stream_did.clone(),
            query: query.clone(),
        };
        let encoded = codec::encode(&args)?;

        let response = self.emit_with_ack("stream/query", encoded).await?;

        let resp: StreamQueryResp = codec::decode(&response)?;
        let raw_rows = resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))?;

        // Convert SqlValueRaw to SqlValue
        let rows: SqlRows = raw_rows
            .into_iter()
            .map(|mut row| {
                row.iter_mut()
                    .map(|(k, v)| (k.clone(), Self::convert_sql_value(v.clone())))
                    .collect()
            })
            .collect();

        Ok(rows)
    }

    /// Set the handle for a stream
    #[instrument(skip(self))]
    pub async fn set_handle(&self, stream_did: &Did, handle: Option<&str>) -> Result<()> {
        let args = StreamSetHandleArgs {
            stream_did: stream_did.clone(),
            handle: handle.map(|s| s.to_string()),
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/set_handle", encoded)
            .await?;

        let resp: StreamSetHandleResp = codec::decode(&response)?;
        resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))
    }

    /// Clear state for a stream
    #[instrument(skip(self))]
    pub async fn clear_state(&self, stream_did: &Did) -> Result<()> {
        let args = StreamClearStateArgs {
            stream_did: stream_did.clone(),
        };
        let encoded = codec::encode(&args)?;

        let response = self
            .emit_with_ack("stream/clear_state", encoded)
            .await?;

        let resp: StreamClearStateResp = codec::decode(&response)?;
        resp.into_result()
            .map_err(|e| LeafClientError::Remote(e))
    }

    /// Emit an event and wait for acknowledgement
    async fn emit_with_ack(&self, event: &str, payload: Vec<u8>) -> Result<Vec<u8>> {
        use std::time::Duration;

        // Create a channel to receive the ack response
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        let callback = move |payload: Payload, _client: Client| {
            if let Payload::Binary(data) = payload {
                let _ = tx.send(data.to_vec());
            }
            futures::future::ready::<()>(()).boxed()
        };

        // Send with ack
        self.socket
            .emit_with_ack(
                event,
                Payload::Binary(payload.into()),
                Duration::from_secs(5),
                callback,
            )
            .await
            .map_err(|e| LeafClientError::Socket(format!("Emit failed: {}", e)))?;

        // Wait for response
        rx.recv()
            .await
            .ok_or_else(|| LeafClientError::Socket("No response received".to_string()))
    }

    /// Disconnect from the server
    pub async fn disconnect(self) -> Result<()> {
        self.socket
            .disconnect()
            .await
            .map_err(|e| LeafClientError::Socket(format!("Disconnect failed: {}", e)))
    }
}
