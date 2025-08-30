use rmpv::Value;
use serde_json::json;
use socketioxide::extract::{AckSender, Data, SocketRef, TryData};
use tracing::{Instrument, Span};

use crate::storage::STORAGE;

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
                Ok(hash) => ack.send(&json!({ "hash": hash.to_hex().to_string() })).ok(),
                Err(e) => ack.send(&json!({ "error": e.to_string()})).ok(),
            };
        },
    );

    socket.on("stream/create", async move || {});

    socket.on(
        "message-with-ack",
        async |Data::<Value>(data), ack: AckSender| {
            tracing::info!(?data, "Received event");
            ack.send(&data).ok();
        },
    );
}
