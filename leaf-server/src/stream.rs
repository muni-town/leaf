use parity_scale_codec::Encode;

pub type RawHash = [u8; blake3::OUT_LEN];

/// A filter policy: either block or allow an event.
#[derive(Encode, serde::Deserialize, Debug, Eq, PartialEq, Hash)]
pub enum Policy {
    Block,
    Allow,
}

#[derive(Encode, Debug)]
pub struct SerdeHash(RawHash);

impl<'de> serde::Deserialize<'de> for SerdeHash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let mut h: RawHash = [0; 32];
        hex::decode_to_slice(s, &mut h).map_err(|e| D::Error::custom(format!("{e}")))?;
        Ok(Self(h))
    }
}

/// The genesis configuration of an event stream.
#[derive(Encode, serde::Deserialize, Debug)]
pub struct GenesisStreamConfig {
    /// User ID of the user that created the stream.
    creator: String,
    /// Unix timestamp of when the stream was created.
    timestamp: i64,
    /// The list of hashes of the WASM inbound filters that will be run on every event for this stream.
    inbound_filters: Vec<SerdeHash>,
    /// The list of hashes of outbound WASM filters that will be run on every event for this stream.
    outbound_filters: Vec<SerdeHash>,
    /// The default rule for inbound events.
    default_inbound_policy: Policy,
    /// The default rule for outbound events.
    default_outbound_policy: Policy,
}

impl GenesisStreamConfig {
    /// Compute the stream ID of this stream based on it's genesis config.
    pub fn get_stream_id(&self) -> blake3::Hash {
        let encoded = self.encode();
        blake3::hash(&encoded)
    }
}
