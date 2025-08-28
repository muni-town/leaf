use std::path::PathBuf;

#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// The Iggy connection string
    #[arg(
        short = 'I',
        long,
        env,
        default_value = "iggy://iggy:password@localhost:8090"
    )]
    pub iggy_url: String,
    #[arg(short = 'l', long, env, default_value = "0.0.0.0:5530")]
    pub listen_address: String,
    #[arg(short = 'd', long, env, default_value = "./data")]
    pub data_dir: PathBuf,
}
