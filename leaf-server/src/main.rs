use std::sync::LazyLock;

use clap::Parser;
use futures_util::StreamExt;

use crate::cli::Args;

mod cli;
mod iggy;
mod otel;

static ARGS: LazyLock<Args> = LazyLock::new(Args::parse);

#[tokio::main]
#[tracing::instrument]
async fn main() {
    // Parse CLI arguments
    let _ = &*ARGS;

    // Initialize logging & telemetry
    let g = otel::init();

    let meter = g.meter_provider.clone();
    let tracer = g.tracer_provider.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        if let Err(e) = meter.force_flush() {
            eprintln!("Error shutting down otel meter: {e}");
        }
        if let Err(e) = tracer.force_flush() {
            eprintln!("Error shutting down otel tracer: {e}");
        }
        if let Err(e) = meter.shutdown() {
            eprintln!("Error shutting down otel meter: {e}");
        }
        if let Err(e) = tracer.shutdown() {
            eprintln!("Error shutting down otel tracer: {e}");
        }
        std::process::exit(0);
    });

    let result: anyhow::Result<()> = async {
        start_server().await?;

        Ok(())
    }
    .await;

    if let Err(e) = result {
        tracing::error!("Error starting server: {e}");
    }
}

#[tracing::instrument(err)]
async fn start_server() -> anyhow::Result<()> {
    tracing::info!(args=?&*ARGS, "Starting Leaf server");

    let iggy = self::iggy::build_client().await?;

    let mut consumer = iggy
        .consumer("leaf-server-test", "leaf", "test", 1)?
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .build();

    consumer.init().await?;

    tokio::spawn(async move {
        while let Some(message) = consumer.next().await {
            match message {
                Ok(message) => {
                    let message = message
                        .message
                        .payload_as_string()
                        .unwrap_or_else(|_| "[binary]".to_string());
                    tracing::info!(?message, "Recieved message from Iggy")
                }
                Err(e) => tracing::error!("Error getting message from Iggy: {e}"),
            }
        }
    });

    Ok(())
}
