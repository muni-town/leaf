//! Error types for the Leaf client

use std::io;

use thiserror::Error;

/// Errors that can occur in the Leaf client
#[derive(Debug, Error)]
pub enum LeafClientError {
    /// DID format validation failed
    #[error("Invalid DID format: {0}")]
    InvalidDid(String),

    /// CBOR encoding/decoding error
    #[error("CBOR error: {0}")]
    Cbor(String),

    /// Socket.IO communication error
    #[error("Socket error: {0}")]
    Socket(String),

    /// Protocol error from the server
    #[error("Remote error: {0}")]
    Remote(String),

    /// Invalid response from server
    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    /// I/O error
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Result type alias for Leaf client operations
pub type Result<T> = std::result::Result<T, LeafClientError>;
