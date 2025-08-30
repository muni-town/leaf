use std::sync::LazyLock;

use iggy::prelude::{Client, IggyClient};
use tracing::instrument;

use crate::{ARGS, async_oncelock::AsyncOnceLock};

pub static IGGY: LazyLock<IggyConnection> = LazyLock::new(IggyConnection::default);

#[derive(Default)]
pub struct IggyConnection {
    client: AsyncOnceLock<IggyClient>,
}

impl IggyConnection {
    #[instrument(skip(self), err)]
    pub async fn initialize(&self) -> anyhow::Result<()> {
        if self.client.has_initialized() {
            tracing::warn!("Iggy client already initialized.");
            return Ok(());
        }

        // Connect to Iggy
        let client = IggyClient::from_connection_string(&ARGS.iggy_url)?;
        client.connect().await?;

        self.client.set(client).ok();

        Ok(())
    }
}
