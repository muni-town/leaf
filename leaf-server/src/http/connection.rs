use std::{collections::HashSet, sync::Arc};

use base64::Engine;
use blake3::Hash;
use leaf_stream::StreamGenesis;
use rmpv::Value;
use serde::Deserialize;
use serde_json::json;
use socketioxide::extract::{AckSender, Data, SocketRef, TryData};
use tokio::sync::RwLock;
use tracing::{Instrument, Span};
use ulid::Ulid;

use crate::{error::LogError, storage::STORAGE};

pub fn setup_socket_handlers(socket: &SocketRef, did: String) {
    let span = Span::current();

    let open_streams = Arc::new(RwLock::new(HashSet::new()));

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
    socket.on(
        "stream/create",
        async move |TryData::<StreamCreateInput>(data), ack: AckSender| {
            let result = async {
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
                    open_streams.write().await.insert(stream);
                    ack.send(&json!({ "stream_id": id.to_hex().as_str() }))
                }
                Err(e) => ack.send(&json!({ "error": e.to_string()})),
            }
            .log_error("Internal error sending response")
            .ok();
        },
    );

    socket.on(
        "message-with-ack",
        async |Data::<Value>(data), ack: AckSender| {
            tracing::info!(?data, "Received event");
            ack.send(&data).ok();
        },
    );
}

#[derive(Deserialize)]
struct StreamCreateInput {
    /// The hex-encoded, blake3 hash of the WASM module to use to create the stream.
    module: String,
    /// The base64-encoded parameters to configure the module with.
    params: String,
}
