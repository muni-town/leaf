//! Demonstration of the Leaf client API
//!
//! This example shows how to use the Rust Leaf client to connect to a server
//! and perform various operations.

use leaf_client_rust::{LeafClient, types::*};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌿 Leaf Client Rust - API Demo\n");

    // Note: This won't actually connect without a running Leaf server
    // but demonstrates the API usage

    println!("Example 1: Client Connection");
    println!("==============================\n");

    println!("To connect to a Leaf server:");
    println!("```rust");
    println!("let client = LeafClient::connect(");
    println!("    \"http://localhost:5530\",");
    println!("    None::<fn() -> futures::future::Ready<Result<String, _>>>,");
    println!(").await?;");
    println!("```");

    println!("\nExample 2: Creating a Query");
    println!("==============================\n");

    let mut params = HashMap::new();
    params.insert(
        "user_id".to_string(),
        SqlValueRaw::Text {
            value: "did:web:example.com".to_string(),
        },
    );
    params.insert("limit".to_string(), SqlValueRaw::Integer { value: 10 });

    let query = LeafQuery {
        name: "get_messages".to_string(),
        params,
        start: Some(0),
        limit: Some(50),
    };

    println!("Query constructed: {:?}", query);
    println!("Would be CBOR-encoded and sent via Socket.IO");

    println!("\nExample 3: DID Type Safety");
    println!("==============================\n");

    let did = Did::new("did:web:example.com".to_string())?;
    println!("Valid DID: {:?}", did);

    let invalid_did = Did::new("not-a-did".to_string());
    println!("Invalid DID rejected: {:?}", invalid_did);

    println!("\nExample 4: Available Operations");
    println!("=================================\n");

    println!("Client supports:");
    println!("  - upload_module(module) → Upload module to server");
    println!("  - has_module(module_cid) → Check if module exists");
    println!("  - create_stream(module_cid) → Create new stream");
    println!("  - stream_info(stream_did) → Get stream information");
    println!("  - update_module(stream_did, module_cid) → Update stream module");
    println!("  - send_events(stream_did, events) → Send events to stream");
    println!("  - send_state_events(stream_did, events) → Send state events");
    println!("  - subscribe_events(stream_did, query, handler) → Subscribe to query");
    println!("  - unsubscribe_events(subscription_id) → Cancel subscription");
    println!("  - query(stream_did, query) → Execute a query");
    println!("  - set_handle(stream_did, handle) → Set stream handle");
    println!("  - clear_state(stream_did) → Clear stream state");
    println!("  - disconnect() → Disconnect from server");

    println!("\n✅ API demonstration complete!");
    println!("\nNote: To test against a real server, start a Leaf server and modify");
    println!("the connection URL in the example code.");

    Ok(())
}
