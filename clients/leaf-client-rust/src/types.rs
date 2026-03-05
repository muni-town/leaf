//! Core type definitions for the Leaf client protocol
//!
//! This module defines the types used for CBOR encoding/decoding of Leaf protocol messages.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Branded type for Decentralized Identifiers
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Did(String);

impl Did {
    /// Create a new DID, validating that it starts with "did:"
    pub fn new(s: String) -> Result<Self, crate::error::LeafClientError> {
        if s.starts_with("did:") {
            Ok(Self(s))
        } else {
            Err(crate::error::LeafClientError::InvalidDid(s))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for Did {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Branded type for subscription IDs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(String);

impl SubscriptionId {
    pub fn new(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Wrapper for CidLink references
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CidLink {
    #[serde(rename = "$link")]
    pub link: String,
}

/// Raw SQL value as received from the server (before BytesWrapper conversion)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum SqlValueRaw {
    #[serde(rename = "muni.town.sqliteValue.null")]
    Null,
    #[serde(rename = "muni.town.sqliteValue.integer")]
    Integer { value: i64 },
    #[serde(rename = "muni.town.sqliteValue.real")]
    Real { value: f64 },
    #[serde(rename = "muni.town.sqliteValue.text")]
    Text { value: String },
    #[serde(rename = "muni.town.sqliteValue.blob")]
    Blob { value: BytesWrapper },
}

/// SQL value with decoded bytes
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum SqlValue {
    #[serde(rename = "muni.town.sqliteValue.null")]
    Null,
    #[serde(rename = "muni.town.sqliteValue.integer")]
    Integer { value: i64 },
    #[serde(rename = "muni.town.sqliteValue.real")]
    Real { value: f64 },
    #[serde(rename = "muni.town.sqliteValue.text")]
    Text { value: String },
    #[serde(rename = "muni.town.sqliteValue.blob")]
    Blob { value: Vec<u8> },
}

/// Wrapper for binary data in CBOR
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BytesWrapper {
    #[serde(with = "serde_bytes")]
    pub buf: Vec<u8>,
}

/// A query to execute on a Leaf stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafQuery {
    pub name: String,
    #[serde(default)]
    pub params: HashMap<String, SqlValueRaw>,
    pub start: Option<u64>,
    pub limit: Option<u64>,
}

/// A single row of query results
pub type SqlRow<V = SqlValue> = HashMap<String, V>;

/// Multiple rows of query results
pub type SqlRows<V = SqlValue> = Vec<SqlRow<V>>;

/// Response from subscribe_events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeEventsResp<V = SqlValue> {
    pub rows: SqlRows<V>,
    pub has_more: bool,
}

/// Generic Result type for protocol responses
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ApiResponse<T> {
    Ok(T),
    Err(String),
}

impl<T> ApiResponse<T> {
    pub fn into_result(self) -> Result<T, String> {
        match self {
            ApiResponse::Ok(v) => Ok(v),
            ApiResponse::Err(e) => Err(e),
        }
    }
}

// Request/Response types for each operation

pub type ModuleUploadArgs = ModuleCodec;
pub type ModuleUploadResp = ApiResponse<ModuleUploadOk>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleUploadOk {
    pub module_cid: CidLink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleExistsArgs {
    pub module_cid: CidLink,
}
pub type ModuleExistsResp = ApiResponse<ModuleExistsOk>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleExistsOk {
    pub module_exists: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamCreateArgs {
    pub module_cid: CidLink,
}
pub type StreamCreateResp = ApiResponse<StreamCreateOk>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamCreateOk {
    pub stream_did: Did,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfoArgs {
    pub stream_did: Did,
}
pub type StreamInfoResp = ApiResponse<StreamInfoOk>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfoOk {
    pub module_cid: Option<CidLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamUpdateModuleArgs {
    pub stream_did: Did,
    pub module_cid: CidLink,
}
pub type StreamUpdateModuleResp = ApiResponse<()>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEventBatchArgs {
    pub stream_did: Did,
    pub events: Vec<BytesWrapper>,
}
pub type StreamEventBatchResp = ApiResponse<()>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSubscribeArgs {
    pub stream_did: Did,
    pub query: LeafQuery,
}
pub type StreamSubscribeResp = ApiResponse<StreamSubscribeOk>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSubscribeOk {
    pub subscription_id: SubscriptionId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSubscribeNotification {
    pub subscription_id: SubscriptionId,
    pub response: ApiResponse<SubscribeEventsResp<SqlValueRaw>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamUnsubscribeArgs {
    pub subscription_id: SubscriptionId,
}
pub type StreamUnsubscribeResp = ApiResponse<StreamUnsubscribeOk>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamUnsubscribeOk {
    pub was_subscribed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamQueryArgs {
    pub stream_did: Did,
    pub query: LeafQuery,
}
pub type StreamQueryResp = ApiResponse<SqlRows<SqlValueRaw>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamSetHandleArgs {
    pub stream_did: Did,
    pub handle: Option<String>,
}
pub type StreamSetHandleResp = ApiResponse<()>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStateEventBatchArgs {
    pub stream_did: Did,
    pub events: Vec<BytesWrapper>,
}
pub type StreamStateEventBatchResp = ApiResponse<()>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamClearStateArgs {
    pub stream_did: Did,
}
pub type StreamClearStateResp = ApiResponse<()>;

// Basic module type (simplified for now)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleCodec {
    #[serde(rename = "$type")]
    pub type_name: String,
    // Additional fields would go here
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_did_validation() {
        assert!(Did::new("did:web:example.com".to_string()).is_ok());
        assert!(Did::new("invalid".to_string()).is_err());
    }

    #[test]
    fn test_sql_value_serialization() {
        let value = SqlValue::Integer { value: 42 };
        let json = serde_json::to_string(&value).unwrap();
        assert!(json.contains("\"$type\":\"muni.town.sqliteValue.integer\""));
    }
}
