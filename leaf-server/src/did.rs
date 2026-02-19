use std::{collections::HashMap, sync::LazyLock};

use leaf_stream::atproto_plc::{Did, DidBuilder, Operation, ServiceEndpoint, SigningKey};
use reqwest::Client;
use serde::Deserialize;

use crate::{ARGS, cli::Command, storage::STORAGE};

static CLIENT: LazyLock<Client> = LazyLock::new(Client::new);

pub async fn create_did(owner: String) -> anyhow::Result<Did> {
    let Command::Server(args) = &ARGS.command else {
        panic!("Invalid command");
    };

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

pub async fn update_did_handle(stream_did: Did, handle: Option<String>) -> anyhow::Result<()> {
    // Fetch the current DID document
    let audit_log_resp = CLIENT
        .get(format!("{}/{stream_did}/log/audit", ARGS.plc_directory))
        .send()
        .await?
        .error_for_status()?;

    // Parse the audit log
    let audit_log: AuditLog = audit_log_resp.json().await?;

    // Get the latest op in the log
    let Some(previous) = audit_log.0.iter().rfind(|x| !x.nullified) else {
        anyhow::bail!(
            "Cannot update DID handle: ould not find previous operation in operation log."
        )
    };

    // Get previous alias list
    let mut also_known_as = match &previous.operation {
        Operation::PlcOperation { also_known_as, .. } => also_known_as.clone(),
        Operation::PlcTombstone { .. } => anyhow::bail!("The DID has been tombstoned"),
        Operation::LegacyCreate { .. } => Vec::new(),
    };
    let rotation_keys = match &previous.operation {
        Operation::PlcOperation { rotation_keys, .. } => rotation_keys.clone(),
        Operation::PlcTombstone { .. } => anyhow::bail!("The DID has been tombstoned"),
        Operation::LegacyCreate { recovery_key, .. } => vec![recovery_key.clone()],
    };
    let verification_methods = match &previous.operation {
        Operation::PlcOperation {
            verification_methods,
            ..
        } => verification_methods.clone(),
        Operation::PlcTombstone { .. } => anyhow::bail!("The DID has been tombstoned"),
        Operation::LegacyCreate { signing_key, .. } => {
            let mut m = HashMap::default();
            m.insert("atproto".into(), signing_key.clone());
            m
        }
    };
    let services = match &previous.operation {
        Operation::PlcOperation { services, .. } => services.clone(),
        Operation::PlcTombstone { .. } => anyhow::bail!("The DID has been tombstoned"),
        Operation::LegacyCreate { service, .. } => {
            let mut m = HashMap::default();
            m.insert(
                "atproto_pds".to_string(),
                ServiceEndpoint {
                    service_type: "AtprotoPersonalDataServer".into(),
                    endpoint: service.clone(),
                },
            );
            m
        }
    };

    // Remove any existing leaf:// handle
    also_known_as.retain(|alias| !alias.starts_with("leaf://"));

    // Add a new leaf handle if specified
    if let Some(handle) = handle {
        also_known_as.push(format!("leaf://{handle}"));
    }

    // Convert to an operation that we will use to update the DID doc
    let unsigned_operation = Operation::new_update(
        rotation_keys,
        verification_methods,
        also_known_as,
        services,
        previous.cid.clone(),
    );

    // Get the signing key for this DID
    let signing_key = STORAGE.get_did_signing_key(stream_did.clone()).await?;

    // Sign the update operation
    let operation = unsigned_operation.sign(&signing_key)?;

    // Submit the updated operation to the PLC directory
    let resp = CLIENT
        .post(format!("{}/{stream_did}", ARGS.plc_directory))
        .json(&operation)
        .send()
        .await?;

    // Verify the request status
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!("PLC directory error: {status}: {body}");
    }

    Ok(())
}

#[derive(Deserialize)]
struct AuditLog(Vec<AuditLogEntry>);

#[derive(Deserialize)]
struct AuditLogEntry {
    #[allow(unused)]
    did: String,
    cid: String,
    nullified: bool,
    operation: Operation,
    #[serde(rename = "createdAt")]
    #[allow(unused)]
    created_at: String,
}
