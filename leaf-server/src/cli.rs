#[derive(clap::Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// The Iggy connection string
    #[arg(short = 'I', long, env, default_value = "iggy://iggy:password@localhost:8090")]
    pub iggy_url: String,
}
