use std::{sync::Arc, time::Duration};

use iggy::prelude::IggyClient;
use rmpv::Value;
use salvo::prelude::*;
use socketioxide::{
    SocketIo,
    extract::{AckSender, Data, SocketRef},
};
use tokio::sync::Notify;
use tracing::{Span, instrument};

use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;

use crate::{ARGS, EXIT_SIGNAL};

#[instrument(err, skip(_iggy))]
pub async fn start_api(_iggy: IggyClient) -> anyhow::Result<()> {
    let acceptor = TcpListener::new(&ARGS.listen_address).bind().await;

    let (layer, io) = SocketIo::new_layer();

    // This code is used to integrates other tower layers before or after Socket.IO such as CORS
    // Beware that classic salvo request won't pass through these layers
    let layer = ServiceBuilder::new()
        .layer(CorsLayer::permissive()) // Enable CORS policy
        .layer(layer); // Mount Socket.IO

    let router = Router::new()
        .push(Router::with_path("/socket.io").goal(layer.compat()))
        .push(Router::new().get(index));

    let server = Server::new(acceptor);
    let handle = server.handle();

    io.ns("/", socket_io_connection);

    tokio::spawn(async move {
        EXIT_SIGNAL.wait_for_exit_signal().await;
        handle.stop_graceful(Some(Duration::from_secs(5)));
    });

    tokio::spawn(server.serve(router));

    // let mut consumer = iggy
    //     .consumer("leaf-server-test", "leaf", "test", 1)?
    //     .create_consumer_group_if_not_exists()
    //     .auto_join_consumer_group()
    //     .build();

    // consumer.init().await?;

    // tokio::spawn(async move {
    //     while let Some(message) = consumer.next().await {
    //         match message {
    //             Ok(message) => {
    //                 let message = message
    //                     .message
    //                     .payload_as_string()
    //                     .unwrap_or_else(|_| "[binary]".to_string());
    //                 tracing::info!(?message, "Recieved message from Iggy")
    //             }
    //             Err(e) => tracing::error!("Error getting message from Iggy: {e}"),
    //         }
    //     }
    // });

    Ok(())
}

#[handler]
async fn index() -> &'static str {
    "Leaf Server API"
}

#[instrument(skip(socket))]
async fn socket_io_connection(socket: SocketRef, Data(data): Data<Value>) {
    let span = Span::current();
    socket.emit("auth", &data).ok();

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

    // Wait for disconnect, this is important to make sure the socket_io_connection tracing span
    // lasts longer than it's children and is recorded as such.
    let notify = Arc::new(Notify::new());
    let notify_ = notify.clone();
    socket.on_disconnect(move || {
        notify_.notify_one();
    });
    notify.notified().await;
}
