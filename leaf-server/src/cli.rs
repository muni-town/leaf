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

    /// Directory to store the leaf data in
    #[arg(short = 'd', long, env, default_value = "./data")]
    pub data_dir: PathBuf,

    /// Set the PLC directory to use
    #[arg(long, env, default_value = "http://localhost:3001")]
    pub plc_directory: String,

    /// List of admin DIDs that are allowed to change the module for ANY stream.
    /// Accepts both `did:plc:` and `did:web:` DIDs.
    #[arg(long, env, value_delimiter = ',', value_parser = parse_admin_did)]
    pub module_admins: Vec<String>,

    #[clap(subcommand)]
    pub command: Command,
}

fn parse_admin_did(s: &str) -> Result<String, String> {
    if s.starts_with("did:plc:") || s.starts_with("did:web:") {
        Ok(s.to_string())
    } else {
        Err(format!(
            "expected a `did:plc:` or `did:web:` DID, got `{s}`"
        ))
    }
}

#[derive(clap::Subcommand, Debug)]
pub enum Command {
    Server(ServerArgs),
    Backup(BackupArgs),
}

#[derive(clap::Parser, Debug)]
pub struct BackupArgs {
    #[clap(subcommand)]
    pub command: BackupCommand,
}

/// Restore from backup or reset backup cache.
#[derive(clap::Subcommand, Debug)]
pub enum BackupCommand {
    /// Resets the local cache of which data has been backed up.
    ///
    /// The server keeps a local cache of which data has been backed up, so it knows what needs to
    /// be backed up when new data comes in.
    ///
    /// This command will reset the server's cache of the state of the backup so that all data will
    /// be backed up again.
    ///
    /// For example, if you change the backup s3 bucket or clear out the backup data, this command
    /// **must** be called to make sure that the server knows it must re-backup **all** the old
    /// data, not just new data.
    ResetBackupCache,
    /// Completely restore the server's data from a backup endpoint.
    ///
    /// This is a potentially destructive operation, so the data dir should be copied / backed up
    /// before trying to run a restore.
    Restore {
        #[clap(flatten)]
        backup_config: S3BackupConfig,
    },
}

/// Run the leaf server.
#[derive(clap::Parser)]
pub struct ServerArgs {
    /// The address to start the server on
    #[arg(short = 'l', long, env, default_value = "0.0.0.0:5530")]
    pub listen_address: String,
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

    #[clap(flatten)]
    pub backup_config: OptionalS3BackupConfig,
}

impl std::fmt::Debug for ServerArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerArgs")
            .field("listen_address", &self.listen_address)
            .field("did", &self.did)
            .field("endpoint", &self.endpoint)
            .field(
                "unsafe_auth_token",
                &self.unsafe_auth_token.as_ref().map(|_| "***REDACTED***"),
            )
            .field("backup_config", &self.backup_config)
            .finish()
    }
}

#[derive(Clone, clap::Args)]
#[group(
    required = false,
    multiple = true,
    requires_all = ["host", "name", "region", "access_key", "secret_key"],
    id = "backup_args"
)]
pub struct OptionalS3BackupConfig {
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

impl std::fmt::Debug for OptionalS3BackupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptionalS3BackupConfig")
            .field("host", &self.host)
            .field("name", &self.name)
            .field("region", &self.region)
            .field("access_key", &self.access_key.as_ref().map(|_| "***REDACTED***"))
            .field("secret_key", &self.secret_key.as_ref().map(|_| "***REDACTED***"))
            .finish()
    }
}

impl From<OptionalS3BackupConfig> for S3BackupConfig {
    fn from(v: OptionalS3BackupConfig) -> Self {
        Self {
            host: v.host.unwrap(),
            name: v.name.unwrap(),
            region: v.region.unwrap(),
            access_key: v.access_key.unwrap(),
            secret_key: v.secret_key.unwrap(),
        }
    }
}
