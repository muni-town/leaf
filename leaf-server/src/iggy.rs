use std::{collections::HashSet, sync::LazyLock};

use iggy::prelude::{
    Client, CompressionAlgorithm, Identifier, IggyClient, IggyExpiry, IggyMessage, MaxTopicSize,
    StreamClient, TopicClient,
};
use tracing::instrument;

use crate::{ARGS, async_oncelock::AsyncOnceLock, storage::STORAGE, stream::GenesisStreamConfig};

pub static IGGY: LazyLock<IggyConnection> = LazyLock::new(IggyConnection::default);

#[derive(Default)]
pub struct IggyConnection {
    client: AsyncOnceLock<IggyClient>,
}

const DEFAULT_STREAM: &str = "default";

impl IggyConnection {
    async fn client(&self) -> &IggyClient {
        (&self.client).await
    }

    #[instrument(skip(self), err)]
    pub async fn initialize(&self) -> anyhow::Result<()> {
        if self.client.has_initialized() {
            tracing::warn!("Iggy client already initialized.");
            return Ok(());
        }

        // Connect to Iggy
        let client = IggyClient::from_connection_string(&ARGS.iggy_url)?;
        client.connect().await?;

        let default_id = Identifier::from_str_value(DEFAULT_STREAM)?;
        if client.get_stream(&default_id).await?.is_none() {
            client.create_stream(DEFAULT_STREAM, None).await?;
        }

        self.client.set(client).ok();

        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn create_stream(
        &self,
        genesis: GenesisStreamConfig,
    ) -> anyhow::Result<blake3::Hash> {
        let client = self.client().await;
        let (hash, encoded) = genesis.get_stream_id_and_bytes();
        let id = &hash.to_hex().to_string();

        // Make sure we have all the WASM modules
        let modules = genesis.wasm_blobs();

        let existing_modules = STORAGE.find_wasm_blobs(&modules).await?;
        let missing_modules = modules
            .difference(&existing_modules)
            .collect::<HashSet<_>>();
        if !missing_modules.is_empty() {
            anyhow::bail!(
                "Some WASM modules missing on server and must be uploaded first: {missing_modules:?}"
            );
        }

        // Create a new iggy topic for the stream
        client
            .create_topic(
                &Identifier::from_str_value(DEFAULT_STREAM)?,
                id,
                1,
                CompressionAlgorithm::Gzip,
                None,
                None,
                IggyExpiry::NeverExpire,
                MaxTopicSize::Unlimited,
            )
            .await?;

        let producer = client.producer(DEFAULT_STREAM, id)?.build();
        producer.init().await?;

        // Send the genesis message to the stream
        producer
            .send_one(IggyMessage::builder().payload(encoded.into()).build()?)
            .await?;

        Ok(hash)
    }
}
