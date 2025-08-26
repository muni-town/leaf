use std::time::Duration;

use iggy::prelude::IggyClient;
use salvo::prelude::*;
use tracing::instrument;

use crate::{ARGS, EXIT_SIGNAL};

#[instrument(err, skip(iggy))]
pub async fn start_api(iggy: IggyClient) -> anyhow::Result<()> {
    let acceptor = TcpListener::new(&ARGS.listen_address).bind().await;

    let router = Router::new().get(index);

    let server = Server::new(acceptor);
    let handle = server.handle();

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
