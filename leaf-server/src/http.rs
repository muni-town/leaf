use std::{
    sync::{Arc, LazyLock},
    time::Duration,
};

use atproto_identity::model::VerificationMethod;
use atproto_oauth::{encoding::FromBase64, jwt::Claims};
use rmpv::Value;
use salvo::prelude::*;
use socketioxide::{
    ParserConfig, SocketIo,
    extract::{Data, SocketRef},
};
use tokio::sync::Notify;
use tracing::{Span, instrument};

use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;

mod connection;

use crate::{EXIT_SIGNAL, cli::ServerArgs};

#[instrument(err)]
pub async fn start_api(args: &'static ServerArgs) -> anyhow::Result<()> {
    let acceptor = TcpListener::new(&args.listen_address).bind().await;

    let (layer, io) = SocketIo::builder()
        .with_parser(ParserConfig::msgpack())
        .build_layer();

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

pub static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(reqwest::Client::new);

#[instrument(skip(socket, data))]
async fn socket_io_connection(socket: SocketRef, Data(data): Data<Value>) {
    // Get the auth token from incomming connection
    let did = async move {
        let token = get_token(&data)?;
        let did = verify_auth_token(token).await?;
        anyhow::Ok(did)
    }
    .await;
    let did = match did {
        Ok(did) => did,
        Err(error) => {
            socket
                .emit("error", &format!("Error validating auth token: {error}"))
                .ok();
            if let Err(error) = socket.disconnect() {
                tracing::error!(%error, "Error disconnecting socket");
            }
            return;
        }
    };

    tracing::info!(%did, "Successfully authenticated user");
    let span = Span::current();
    span.set_attribute("did", did.clone());

    // Send authenticated message to client
    socket
        .emit(
            "authenticated",
            &serde_json::json!({
                "did": did,
            }),
        )
        .ok();

    connection::setup_socket_handlers(&socket, did);

    // Wait for disconnect, this is important to make sure the socket_io_connection tracing span
    // lasts longer than it's children and is recorded as such.
    let notify = Arc::new(Notify::new());
    let notify_ = notify.clone();
    socket.on_disconnect(move || {
        notify_.notify_one();
    });
    notify.notified().await;
}

/// Extract the auth token from the incomming socket.io connection data
fn get_token(data: &Value) -> anyhow::Result<&str> {
    data.as_map()
        .and_then(|m| {
            m.iter().find_map(|x| match (x.0.as_str(), x.1.as_str()) {
                (Some("token"), Some(value)) => Some(value),
                _ => None,
            })
        })
        .ok_or_else(|| anyhow::format_err!("Auth token not found in socket.io connection"))
}

/// Validate that an ATProto JWT auth token is valid.
#[instrument(skip(token), err)]
async fn verify_auth_token(token: &str) -> anyhow::Result<String> {
    let claims_base64 = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::format_err!("Invalid format for JWT auth token"))?;
    let claims = Claims::from_base64(claims_base64)?;
    let Some(did) = claims.jose.issuer else {
        anyhow::bail!("JWT token issuer is missing")
    };
    Span::current().set_attribute("did", did.clone());

    let doc = atproto_identity::plc::query(&CLIENT, "plc.directory", &did).await?;

    let public_key_multibase = doc
        .verification_method
        .into_iter()
        .find_map(|x| match x {
            VerificationMethod::Multikey {
                public_key_multibase,
                ..
            } => Some(public_key_multibase),
            _ => None,
        })
        .ok_or_else(|| anyhow::format_err!("Could not find signing key for DID: {did}"))?;

    let key_data = atproto_identity::key::identify_key(&public_key_multibase)?;

    atproto_oauth::jwt::verify(token, &key_data)?;

    Ok(did)
}
