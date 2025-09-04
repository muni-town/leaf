use std::{
    path::PathBuf,
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use base64::Engine;
use clap::{Parser, Subcommand};
use leaf_stream::{
    Stream, StreamGenesis, encoding::Encodable, libsql, modules::wasm::LeafWasmModule, ulid::Ulid,
};
use salvo::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

/// Leaf framework development CLI.
#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Host an API endpoint for testing a leaf stream
    Host(HostArgs),
}

#[derive(Parser, Debug)]
struct HostArgs {
    /// The address and port to have the webserver listen on.
    #[arg(short = 'l', long, default_value = "127.0.0.1:9938")]
    listen_address: String,
    /// The path to the stream's config TOML file.
    #[arg(default_value = "stream.toml")]
    config_file: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct StreamConfigToml {
    pub ulid: Ulid,
    pub creator: String,
    pub module: PathBuf,
    pub params: PathBuf,
    pub stream_db: PathBuf,
    pub module_db: PathBuf,
}

static ARGS: LazyLock<Args> = LazyLock::new(Args::parse);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let args = &*ARGS;
    tracing::debug!(?args, "Parsed CLI arguments");

    match &args.command {
        Commands::Host(host_args) => host(host_args).await?,
    }

    Ok(())
}

struct Inject<T>(T);
#[async_trait::async_trait]
impl<T: Clone + Sync + Send + 'static> Handler for Inject<T> {
    async fn handle(
        &self,
        _req: &mut Request,
        depot: &mut Depot,
        _res: &mut Response,
        _ctrl: &mut FlowCtrl,
    ) {
        depot.inject(self.0.clone());
    }
}

async fn host(args: &HostArgs) -> anyhow::Result<()> {
    let config_file_str = tokio::fs::read_to_string(&args.config_file)
        .await
        .context("Could not open stream config file")?;
    let config: StreamConfigToml =
        toml::de::from_str(&config_file_str).context("Could not parse config file")?;
    let module_db_filepath = args.config_file.parent().unwrap().join(config.module_db);
    let module_db = libsql::Builder::new_local(module_db_filepath)
        .build()
        .await?
        .connect()?;
    let stream_db_filepath = args.config_file.parent().unwrap().join(config.stream_db);
    let stream_db = libsql::Builder::new_local(stream_db_filepath)
        .build()
        .await?
        .connect()?;

    let module_filepath = args.config_file.parent().unwrap().join(config.module);
    let module_bytes = tokio::fs::read(module_filepath)
        .await
        .context("Could not open module file")?;
    let params_filepath = args.config_file.parent().unwrap().join(config.params);
    let params = tokio::fs::read(params_filepath)
        .await
        .context("Could not open params file")?;
    let module_id = blake3::hash(&module_bytes);

    let module = LeafWasmModule::new(&module_bytes)?;

    let acceptor = TcpListener::new(args.listen_address.clone()).bind().await;

    let genesis = StreamGenesis {
        stamp: Encodable(config.ulid),
        creator: config.creator,
        module: Encodable(module_id),
        params,
    };

    let mut stream = leaf_stream::Stream::open(genesis, stream_db).await?;
    stream.provide_module(Box::new(module), module_db)?;
    tracing::info!(status=?stream.module(), id=?stream.id(), "Successfully opened stream");

    let router = Router::new()
        .hoop(Inject(Arc::new(RwLock::new(stream))))
        .post(post_event)
        .push(Router::new().get(get_events));

    Server::new(acceptor).serve(router).await;

    Ok(())
}

type StreamCtx = Arc<RwLock<Stream>>;

#[derive(Serialize)]
struct Event {
    pub index: u64,
    pub user: String,
    pub payload: Payload,
}

enum Payload {
    String(String),
    Binary(Vec<u8>),
}

impl serde::Serialize for Payload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct StringPayload<'a> {
            string: &'a str,
        }
        #[derive(Serialize)]
        struct Base64<'a> {
            base64: &'a str,
        }
        match self {
            Payload::String(s) => StringPayload { string: s }.serialize(serializer),
            Payload::Binary(items) => Base64 {
                base64: &base64::prelude::BASE64_STANDARD.encode(items),
            }
            .serialize(serializer),
        }
    }
}

#[handler]
async fn get_events(
    depot: &mut Depot,
    req: &mut Request,
    res: &mut Response,
) -> anyhow::Result<()> {
    let stream = depot
        .obtain::<StreamCtx>()
        .expect("Missing stream context")
        .read()
        .await;
    let offset = req.query::<u64>("offset").unwrap_or(0);
    let limit = req.query::<u64>("limit").unwrap_or(100);

    let events = stream.fetch_events("anonymous:", offset, limit).await?;

    let events = events
        .into_iter()
        .map(|x| Event {
            index: x.idx,
            user: x.user,
            payload: match String::from_utf8(x.payload) {
                Ok(s) => Payload::String(s),
                Err(e) => Payload::Binary(e.into_bytes()),
            },
        })
        .collect::<Vec<_>>();

    res.render(Json(events));

    Ok(())
}

#[handler]
async fn post_event(depot: &mut Depot, req: &mut Request) -> anyhow::Result<()> {
    let mut stream = depot
        .obtain::<StreamCtx>()
        .expect("Missing stream context")
        .write()
        .await;
    let payload = req.payload().await?.to_vec();
    let user = req
        .header::<String>("user")
        .unwrap_or_else(|| "anonymous:".into());

    let new_module = stream.handle_event(user, payload).await?;

    tracing::info!("Successfully processed event");
    if new_module.is_some() {
        anyhow::bail!(
            "Event was processed successfully, but it triggered a \
            module change which cannot be handled automatically by the dev server. \
            You will need to update the config TOML to point to the new module and \
            restart the server."
        );
    }

    Ok(())
}
