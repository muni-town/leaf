use std::sync::{Arc, LazyLock};

use clap::Parser;
use futures_util::StreamExt;
use tokio::sync::Notify;

use crate::cli::Args;

mod cli;
mod http;
mod iggy;
mod otel;

#[derive(Default)]
struct ExitSignal(Arc<Notify>);

impl ExitSignal {
    async fn wait_for_exit_signal(&self) {
        self.0.notified().await;
    }

    fn trigger_exit_signal(&self) {
        self.0.notify_waiters();
    }
}

static ARGS: LazyLock<Args> = LazyLock::new(Args::parse);
static EXIT_SIGNAL: LazyLock<ExitSignal> = LazyLock::new(ExitSignal::default);

#[tokio::main]
#[tracing::instrument]
async fn main() {
    // Parse CLI arguments
    let _ = &*ARGS;

    // Initialize logging & telemetry
    let _g = otel::init();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        EXIT_SIGNAL.trigger_exit_signal();
    });

    let result: anyhow::Result<()> = async {
        start_server().await?;

        wait_for_shutdown().await;

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

    http::start_api(iggy).await?;

    Ok(())
}

#[tracing::instrument]
async fn wait_for_shutdown() {
    EXIT_SIGNAL.wait_for_exit_signal().await;
    tracing::info!("Received exit signal, shutting down.");
}
