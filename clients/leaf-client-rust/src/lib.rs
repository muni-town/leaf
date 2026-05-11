//! Leaf Client - Rust implementation of the Leaf protocol client
//!
//! This is a Rust port of the TypeScript Leaf client, providing a type-safe
//! and performant interface for interacting with Leaf servers.

pub mod client;
pub mod codec;
pub mod error;
pub mod types;

// Re-export commonly used types
pub use client::LeafClient;
pub use error::{LeafClientError, Result};
pub use types::{
    Did, LeafQuery, SqlRow, SqlRows, SqlValue, SqlValueRaw, SubscriptionId,
};

#[cfg(test)]
mod tests {
    #[test]
    fn test_library_builds() {
        // Basic test to ensure the library compiles
        assert!(true);
    }
}
