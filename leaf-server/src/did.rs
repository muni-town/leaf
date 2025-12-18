use std::sync::LazyLock;

use leaf_stream::atproto_plc::{Did, DidBuilder, ServiceEndpoint, SigningKey};
use reqwest::Client;

use crate::{ARGS, cli::Command, storage::STORAGE};

static CLIENT: LazyLock<Client> = LazyLock::new(Client::new);

pub async fn create_did(owner: String) -> anyhow::Result<Did> {
    let Command::Server(args) = &ARGS.command;

    let key = SigningKey::generate_k256();
    let (did, operation, keys) = DidBuilder::new()
        .add_rotation_key(key.clone())
        .add_verification_method("leaf_server".to_string(), key)
        .add_service(
            "leaf_server".into(),
            ServiceEndpoint {
                endpoint: args.endpoint.clone(),
                service_type: "LeafServer".into(),
            },
        )
        .build()?;
    let key = keys.rotation_keys.into_iter().next().unwrap();

    operation.verify(&[key.verifying_key()])?;

    let resp = CLIENT
        .post(format!("{}/{did}", ARGS.plc_directory))
        .json(&operation)
        .send()
        .await?;

    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("PLC directory error: {status}: {body}");
    }

    STORAGE.create_did(did.clone(), key, owner).await?;

    Ok(did)
}
