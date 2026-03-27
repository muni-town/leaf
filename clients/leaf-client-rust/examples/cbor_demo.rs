//! Demonstration of CBOR encoding/decoding
//!
//! This example shows how the Rust client handles CBOR serialization
//! and compares it to the TypeScript implementation.

use leaf_client_rust::{codec, types::*};

fn main() {
    println!("🌿 Leaf Client Rust - CBOR Demo\n");

    // Example 1: Encoding/Decoding SQL values
    println!("Example 1: SQL Value Serialization");
    println!("====================================");

    let integer_value = SqlValue::Integer { value: 42 };
    let encoded = codec::encode(&integer_value).unwrap();
    println!("Integer value: {:?}", integer_value);
    println!("Encoded size: {} bytes", encoded.len());
    println!("Encoded hex: {:02x?}", encoded);

    let decoded: SqlValue = codec::decode(&encoded).unwrap();
    println!("Decoded value: {:?}", decoded);
    println!("Round-trip successful: {}\n", decoded == integer_value);

    // Example 2: Complex query structure
    println!("Example 2: Leaf Query Structure");
    println!("================================");

    let mut params = std::collections::HashMap::new();
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

    let encoded_query = codec::encode(&query).unwrap();
    println!("Query: {:?}", query);
    println!("Encoded size: {} bytes\n", encoded_query.len());

    // Example 3: DID validation
    println!("Example 3: DID Type Safety");
    println!("==========================");

    let valid_did = Did::new("did:web:example.com".to_string());
    println!("Valid DID: {:?}", valid_did);

    let invalid_did = Did::new("not-a-did".to_string());
    println!("Invalid DID: {:?}", invalid_did);
    println!("\n✅ All examples completed successfully!");
}
