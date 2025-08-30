use std::collections::{HashMap, HashSet};

use blake3::Hash;
use rmpv::Value;
use serde_json::json;
use socketioxide::extract::{AckSender, Data, SocketRef, TryData};
use tracing::{Instrument, Span};

use crate::{error::LogError, storage::STORAGE, stream::GenesisStreamConfig};

pub fn setup_socket_handlers(socket: &SocketRef, did: String) {
    let span = Span::current();

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
        async move |TryData::<Vec<String>>(hashes_hex), ack: AckSender| {
            let result = async {
                let hashes_hex = hashes_hex?;
                let hashes = hashes_hex
                    .into_iter()
                    .map(Hash::from_hex)
                    .collect::<Result<HashSet<_>, _>>()?;
                let found_hashes = STORAGE.find_wasm_blobs(&hashes).await?;
                let mut response = HashMap::new();
                for hash in &found_hashes {
                    response.insert(hash.to_hex().to_string(), true);
                }
                for hash in hashes.difference(&found_hashes) {
                    response.insert(hash.to_hex().to_string(), false);
                }
                anyhow::Ok(response)
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
    socket.on(
        "stream/create",
        async move |TryData::<GenesisStreamConfig>(data), ack: AckSender| {
            let result = async {
                let data = data?;
                tracing::warn!(?data, "Stream creation not implemented yet");
                anyhow::Ok(data.get_stream_id())
            }
            .instrument(tracing::info_span!(parent: span_.clone(), "handle stream/create"))
            .await;

            match result {
                Ok(hash) => ack.send(&json!({ "stream_id": hash.to_hex().to_string() })),
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
