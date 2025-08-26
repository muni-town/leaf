use iggy::prelude::{Client, IggyClient};
use tracing::instrument;

use crate::ARGS;

#[instrument(name = "build_iggy_client", err)]
pub async fn build_client() -> anyhow::Result<IggyClient> {
    let client = IggyClient::from_connection_string(&ARGS.iggy_url)?;
    client.connect().await?;
    Ok(client)
}
