use std::path::PathBuf;

use reqwest::Url;

use crate::storage::S3BackupConfig;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Enable open telemetry - TODO: add otel options
    #[arg(long, env)]
    pub otel: bool,
    /// Enable profiling to Pyroscope
    #[arg(long, env)]
    pub profiling: bool,

    /// Set the PLC directory to use
    #[arg(long, env, default_value = "http://localhost:3001")]
    pub plc_directory: String,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand, Debug)]
pub enum Command {
    Server(ServerArgs),
}

/// Run the leaf server.
#[derive(clap::Parser, Debug)]
pub struct ServerArgs {
    /// The address to start the server on
    #[arg(short = 'l', long, env, default_value = "0.0.0.0:5530")]
    pub listen_address: String,
    /// Directory to store the leaf data in
    #[arg(short = 'd', long, env, default_value = "./data")]
    pub data_dir: PathBuf,
    /// The DID of this service
    ///
    /// This is used to validate that ATProto auth JWTs are intended for this service and not
    /// another service.
    ///
    /// It should be set to `did:web:your.public.hostname`.
    #[arg(short = 'D', long, env, default_value = "did:web:localhost")]
    pub did: String,
    /// The public endpoint that this server can be accessed on.
    #[arg(short = 'e', long, env, default_value = "http://localhost:5530")]
    pub endpoint: String,

    // The unsafe auth token allows you to authenticate to the Leaf server with the Leaf server's
    // own DID. If this token is provided during authentication it wll be accepted without any other
    // verification.
    #[arg(long, env)]
    pub unsafe_auth_token: Option<String>,

    /// Window size for per-actor throttling on high-risk socket endpoints.
    #[arg(long, env, default_value_t = 30)]
    pub throttle_window_secs: u64,

    /// Maximum `stream/create` attempts allowed per actor inside one throttle window.
    #[arg(long, env, default_value_t = 8)]
    pub stream_create_limit_per_window: u32,

    /// Maximum `stream/subscribe_events` attempts allowed per actor inside one throttle window.
    #[arg(long, env, default_value_t = 20)]
    pub stream_subscribe_limit_per_window: u32,

    /// Maximum active `stream/subscribe_events` subscriptions per actor.
    #[arg(long, env, default_value_t = 64)]
    pub max_active_subscriptions_per_actor: u32,

    #[clap(flatten)]
    pub backup_config: S3BackupConfigArgs,
}

#[derive(Debug, Clone, clap::Args)]
#[group(
    required = false,
    multiple = true,
    requires_all = ["host", "name", "region", "access_key", "secret_key"])]
pub struct S3BackupConfigArgs {
    #[arg(long = "s3-host", env = "S3_HOST")]
    pub host: Option<Url>,
    #[arg(long = "s3-bucket", env = "S3_BUCKET")]
    pub name: Option<String>,
    #[arg(long = "s3-region", env = "S3_REGION")]
    pub region: Option<String>,
    #[arg(long = "s3-access-key", env = "S3_ACCESS_KEY")]
    pub access_key: Option<String>,
    #[arg(long = "s3-secret-key", env = "S3_SECRET_KEY")]
    pub secret_key: Option<String>,
}

impl From<S3BackupConfigArgs> for S3BackupConfig {
    fn from(v: S3BackupConfigArgs) -> Self {
        Self {
            host: v.host.unwrap(),
            name: v.name.unwrap(),
            region: v.region.unwrap(),
            access_key: v.access_key.unwrap(),
            secret_key: v.secret_key.unwrap(),
        }
    }
}
