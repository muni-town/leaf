use parity_scale_codec::Encode;
use serde::Deserialize;
use ulid::Ulid;

pub type RawHash = [u8; blake3::OUT_LEN];

#[derive(Encode, Clone, Copy, Hash)]
pub struct SerdeRawHash(pub RawHash);
impl std::fmt::Debug for SerdeRawHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        blake3::Hash::from_bytes(self.0).fmt(f)
    }
}

#[derive(Debug, Deserialize)]
pub struct SerdeUlid(pub Ulid);
impl Encode for SerdeUlid {
    fn size_hint(&self) -> usize {
        u128::size_hint(&self.0.0)
    }
    fn encode_to<T: parity_scale_codec::Output + ?Sized>(&self, dest: &mut T) {
        u128::encode_to(&self.0.0, dest)
    }
    fn encode(&self) -> Vec<u8> {
        u128::encode(&self.0.0)
    }
    fn using_encoded<R, F: FnOnce(&[u8]) -> R>(&self, f: F) -> R {
        u128::using_encoded(&self.0.0, f)
    }
    fn encoded_size(&self) -> usize {
        u128::encoded_size(&self.0.0)
    }
}
impl<'de> serde::Deserialize<'de> for SerdeRawHash {
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
