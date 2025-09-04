use std::{collections::HashMap, sync::Arc};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use base64::Engine;
use blake3::Hash;
use leaf_stream::StreamGenesis;
use serde::{Deserialize, Serialize};
use serde_json::json;
use socketioxide::extract::{AckSender, SocketRef, TryData};
use tracing::{Instrument, Span};
use ulid::Ulid;

use crate::{error::LogError, storage::STORAGE};

pub fn setup_socket_handlers(socket: &SocketRef, did: String) {
    let span = Span::current();

    let open_streams = Arc::new(RwLock::new(HashMap::new()));

    let did_ = did.clone();
    let span_ = span.clone();
    socket.on(
        "wasm/upload",
        async move |TryData::<bytes::Bytes>(data), ack: AckSender| {
            let result = async {
                let data = data?;
                let hash = STORAGE.upload_wasm(&did_, data.to_vec()).await?;
                anyhow::Ok(hash)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle wasm/upload"))
            .await;
            match result {
                Ok(hash) => ack.send(&json!({ "hash": hash.to_hex().to_string() })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );
    let span_ = span.clone();
    socket.on(
        "wasm/has",
        async move |TryData::<String>(hash_hex), ack: AckSender| {
            let result = async {
                let hash_hex = hash_hex?;
                let hash = Hash::from_hex(hash_hex)?;
                let has_module = STORAGE.has_blob(hash).await?;
                anyhow::Ok(serde_json::json!(has_module))
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle wasm/has"))
            .await;

            match result {
                Ok(response) => ack.send(&response),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/create",
        async move |TryData::<StreamCreateInput>(data), ack: AckSender| {
            let result = async {
                // Create the stream
                let input = data?;
                let genesis = StreamGenesis {
                    stamp: Ulid::new().into(),
                    creator: did_.clone(),
                    module: Hash::from_hex(input.module)?.into(),
                    params: base64::prelude::BASE64_STANDARD.decode(input.params)?,
                };
                let stream_id = STORAGE.create_stream(genesis).await?;
                anyhow::Ok(stream_id)
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/create"))
            .await;

            match result {
                Ok(stream) => {
                    let id = stream.id();
                    open_streams_.write().await.insert(id, stream);
                    ack.send(&json!({ "stream_id": id.to_hex().as_str() }))
                }
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    socket.on(
        "stream/event",
        async move |TryData::<IncommingEvent>(data), ack: AckSender| {
            let result = async {
                let request = data?;
                let stream_id = Hash::from_hex(request.id)?;
                let open_streams = open_streams_.upgradable_read().await;

                let stream = if let Some(stream) = open_streams.get(&stream_id) {
                    stream.clone()
                } else {
                    let Some(stream) = STORAGE.open_stream(stream_id).await? else {
                        anyhow::bail!("Stream does not exist with ID: {stream_id}");
                    };
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_id, stream.clone());
                    stream
                };

                stream.handle_event(did_.clone(), request.payload).await?;

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/create"))
            .await;

            match result {
                Ok(_) => ack.send(&json!({ "ok": true })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    let span_ = span.clone();
    let did_ = did.clone();
    let open_streams_ = open_streams.clone();
    let socket_ = socket.clone();
    socket.on(
        "stream/event",
        async move |TryData::<String>(data), ack: AckSender| {
            let result = async {
                let stream_id = data?;
                let stream_id = Hash::from_hex(stream_id)?;
                let open_streams = open_streams_.upgradable_read().await;

                let stream = if let Some(stream) = open_streams.get(&stream_id) {
                    stream.clone()
                } else {
                    let Some(stream) = STORAGE.open_stream(stream_id).await? else {
                        anyhow::bail!("Stream does not exist with ID: {stream_id}");
                    };
                    let mut open_streams = RwLockUpgradableReadGuard::upgrade(open_streams).await;
                    open_streams.insert(stream_id, stream.clone());
                    stream
                };

                let mut receiver = stream.subscribe(&did_).await;

                tokio::spawn(async move {
                    while let Ok(event) = receiver.recv().await {
                        if let Err(e) = socket_.emit(
                            "stream/event",
                            &OutgoingEvent {
                                stream: stream_id.to_hex().to_string(),
                                idx: event.idx,
                                user: event.user,
                                payload: event.payload,
                            },
                        ) {
                            // TODO: better error message
                            tracing::error!("Error sending event: {e}");
                        }
                    }
                });

                anyhow::Ok(())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/create"))
            .await;

            match result {
                Ok(_) => ack.send(&json!({ "ok": true })),
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    // TODO: Allow client to fetch events
}

#[derive(Deserialize)]
struct StreamCreateInput {
    /// The hex-encoded, blake3 hash of the WASM module to use to create the stream.
    module: String,
    /// The base64-encoded parameters to configure the module with.
    params: String,
}

#[derive(Deserialize)]
struct IncommingEvent {
    /// The hex-encoded stream ID
    id: String,
    /// The event payload
    payload: Vec<u8>,
}

#[derive(Clone, Debug, Serialize)]
pub struct OutgoingEvent {
    pub stream: String,
    pub idx: i64,
    pub user: String,
    pub payload: Vec<u8>,
}
