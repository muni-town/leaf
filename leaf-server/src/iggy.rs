use iggy::prelude::{Client, IggyClient};
use tracing::instrument;

use crate::ARGS;

#[instrument(name = "connect_to_iggy", err)]
pub async fn connect() -> anyhow::Result<IggyClient> {
    let client = IggyClient::from_connection_string(&ARGS.iggy_url)?;
    client.connect().await?;
    Ok(client)
}
