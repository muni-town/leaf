use std::sync::{Arc, LazyLock};

use clap::Parser;
use tokio::sync::Notify;

use crate::{cli::Args, storage::STORAGE};

mod cli;
mod http;
mod iggy;
mod otel;
mod storage;

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

    // Make Ctrl+C trigger graceful shutdown
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        EXIT_SIGNAL.trigger_exit_signal();
    });

    let result: anyhow::Result<()> = async {
        // Start server
        start_server().await?;

        // Then wait for shutdown signal
        wait_for_shutdown().await;

        Ok(())
    }
    .await;

    // Log server start error
    if let Err(e) = result {
        tracing::error!("Error starting server: {e}");
    }
}

/// Start the leaf server
#[tracing::instrument(err)]
async fn start_server() -> anyhow::Result<()> {
    tracing::info!(args=?&*ARGS, "Starting Leaf server");

    // Initialize storage
    STORAGE.initialize().await?;

    // Connect to Iggy
    let iggy = self::iggy::connect().await?;

    // Start the web API
    http::start_api(iggy).await?;

    Ok(())
}

// Waits for the server shutdown signal then retuns and logs the shutdown.
async fn wait_for_shutdown() {
    EXIT_SIGNAL.wait_for_exit_signal().await;
    let _span = tracing::info_span!("server shutdown").entered();
}
