use std::path::PathBuf;

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
}
