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

    // TODO: add richer request information to tracing.
    let router = Router::new()
        .push(Router::with_path("/socket.io").goal(layer.compat()))
        .push(Router::with_path("/xrpc/space.roomy.token.v0").post(token_endpoint))
        .push(Router::new().get(http_index))
        .hoop(Logger::new());

    let server = Server::new(acceptor);
    let handle = server.handle();

    io.ns("/", socket_io_connection);

    tokio::spawn(async move {
        EXIT_SIGNAL.wait_for_exit_signal().await;
        handle.stop_graceful(Some(Duration::from_secs(5)));
    });

    tokio::spawn(server.serve(router));

    Ok(())
}

#[handler]
#[instrument]
async fn http_index() -> &'static str {
    "Leaf Server API"
}

#[handler]
#[instrument]
async fn token_endpoint() -> &'static str {
    "token"
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
