//! CBOR encoding and decoding utilities
//!
//! This module provides functions for encoding and decoding CBOR data
//! using the ciborium library.

use crate::error::{LeafClientError, Result};
use ciborium::{de::from_reader, ser::into_writer};
use serde::{de::DeserializeOwned, Serialize};
use std::io::Cursor;

/// Encode a value to CBOR format
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    into_writer(value, &mut buf).map_err(|e| LeafClientError::Cbor(e.to_string()))?;
    Ok(buf)
}

/// Decode a value from CBOR format
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let cursor = Cursor::new(bytes);
    from_reader(cursor).map_err(|e| LeafClientError::Cbor(e.to_string()))
}

/// Convert BytesWrapper instances to raw Vec<u8> recursively
///
/// This function walks through a decoded structure and converts any
/// BytesWrapper instances to their underlying byte vectors.
pub fn convert_bytes_wrappers<T>(value: T) -> T {
    // For now, we'll implement this in the types module using serde's custom deserialize
    // This is a placeholder
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SqlValue, SqlValueRaw};

    #[test]
    fn test_encode_decode_sql_value() {
        let original = SqlValue::Integer { value: 42 };
        let encoded = encode(&original).unwrap();
        let decoded: SqlValue = decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_with_bytes() {
        use crate::types::BytesWrapper;

        let original = SqlValueRaw::Blob {
            value: BytesWrapper {
                buf: vec![1, 2, 3, 4],
            },
        };
        let encoded = encode(&original).unwrap();
        let decoded: SqlValueRaw = decode(&encoded).unwrap();

        match decoded {
            SqlValueRaw::Blob { value } => assert_eq!(value.buf, vec![1, 2, 3, 4]),
            _ => panic!("Wrong variant"),
        }
    }
}
