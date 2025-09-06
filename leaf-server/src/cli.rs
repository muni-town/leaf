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
    #[arg(short = 'l', long, env, default_value = "0.0.0.0:5530")]
    pub listen_address: String,
    #[arg(short = 'd', long, env, default_value = "./data")]
    pub data_dir: PathBuf,
}
