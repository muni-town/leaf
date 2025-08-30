use std::collections::HashSet;

use parity_scale_codec::Encode;

use crate::serde::{SerdeRawHash, SerdeUlid};

/// A filter policy: either block or allow an event.
#[derive(Encode, serde::Deserialize, Debug, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Policy {
    Block,
    Allow,
}

/// The genesis configuration of an event stream.
#[derive(Encode, serde::Deserialize, Debug)]
pub struct GenesisStreamConfig {
    /// A ULID, which encompasses the timestamp and additional randomness, included in this stream
    /// to make it's hash unique.
    ///
    /// Note that this is not the stream ID, which is computed fro the hash of the
    /// [`GenesisStreamConfig`].
    pub stamp: SerdeUlid,
    /// User ID of the user that created the stream.
    pub creator: String,
    /// The list of hashes of the WASM inbound filters that will be run on every event for this stream.
    pub inbound_filters: Vec<SerdeRawHash>,
    /// The list of hashes of outbound WASM filters that will be run on every event for this stream.
    pub outbound_filters: Vec<SerdeRawHash>,
    /// The default rule for inbound events.
    pub default_inbound_policy: Policy,
    /// The default rule for outbound events.
    pub default_outbound_policy: Policy,
}

impl GenesisStreamConfig {
    /// Compute the stream ID of this stream based on it's genesis config.
    pub fn get_stream_id_and_bytes(&self) -> (blake3::Hash, Vec<u8>) {
        let encoded = self.encode();
        (blake3::hash(&encoded), encoded)
    }

    pub fn wasm_blobs(&self) -> HashSet<blake3::Hash> {
        self.inbound_filters
            .iter()
            .chain(self.outbound_filters.iter())
            .map(|x| blake3::Hash::from_bytes(x.0))
            .collect::<HashSet<_>>()
    }
}
