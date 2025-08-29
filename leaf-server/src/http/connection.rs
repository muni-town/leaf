use rmpv::Value;
use serde_json::json;
use socketioxide::extract::{AckSender, Data, SocketRef, TryData};
use tracing::Span;

use crate::storage::STORAGE;

pub fn setup_socket_handlers(socket: &SocketRef, did: String) {
    let span = Span::current();

    let did_ = did.clone();
    socket.on(
        "wasm/upload",
        async move |TryData::<bytes::Bytes>(data), ack: AckSender| {
            let result = async {
                let data = data?;
                STORAGE.upload_wasm(&did_, data.to_vec()).await?;
                anyhow::Ok(())
            }
            .await;
            match result {
                Ok(_) => ack.send(&json!({ "ok": true })).ok(),
                Err(e) => ack.send(&json!({ "error": e.to_string()})).ok(),
            };
        },
    );

    let span_ = span.clone();
    socket.on(
        "message",
        async move |socket: SocketRef, Data::<Value>(data)| {
            let _s = tracing::info_span!(parent: &span_, "handle event", %data).entered();

            if data.as_str() == Some("err") {
                tracing::error!("got an error")
            } else {
                socket.emit("message-back", &data).ok();
            }
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
