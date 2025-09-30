use std::path::PathBuf;

use reqwest::Url;

use crate::storage::S3BackupConfig;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Enable open telemetry - TODO: add otel options
    #[arg(long)]
    pub otel: bool,
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
    #[arg(short = 'D', long, env, default_value = "did:web:localhost:5530")]
    pub did: String,

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
