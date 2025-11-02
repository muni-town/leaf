use blake3::Hash;
use leaf_stream_types::Decode;
use parity_scale_codec::Encode;
use ulid::Ulid;

#[derive(Debug, Clone, Copy)]
pub struct Encodable<T>(pub T);
impl<T> From<T> for Encodable<T> {
    fn from(value: T) -> Self {
        Encodable(value)
    }
}
impl Decode for Encodable<Ulid> {
    fn decode<I: parity_scale_codec::Input>(
        input: &mut I,
    ) -> Result<Self, parity_scale_codec::Error> {
        Ok(Encodable(Ulid(u128::decode(input)?)))
    }
}
impl Encode for Encodable<Ulid> {
    fn size_hint(&self) -> usize {
        u128::size_hint(&self.0.0)
    }

    fn encode_to<T: parity_scale_codec::Output + ?Sized>(&self, dest: &mut T) {
        u128::encode_to(&self.0.0, dest);
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
type H = [u8; 32];
impl Encode for Encodable<blake3::Hash> {
    fn size_hint(&self) -> usize {
        H::size_hint(self.0.as_bytes())
    }

    fn encode_to<T: parity_scale_codec::Output + ?Sized>(&self, dest: &mut T) {
        H::encode_to(self.0.as_bytes(), dest);
    }

    fn encode(&self) -> Vec<u8> {
        H::encode(self.0.as_bytes())
    }

    fn using_encoded<R, F: FnOnce(&[u8]) -> R>(&self, f: F) -> R {
        H::using_encoded(self.0.as_bytes(), f)
    }

    fn encoded_size(&self) -> usize {
        H::encoded_size(self.0.as_bytes())
    }
}
impl Decode for Encodable<blake3::Hash> {
    fn decode<I: parity_scale_codec::Input>(
        input: &mut I,
    ) -> Result<Self, parity_scale_codec::Error> {
        Ok(Encodable(Hash::from_bytes(<[u8; 32]>::decode(input)?)))
    }
}

impl<V: Encode> Encode for Encodable<Result<V, anyhow::Error>> {
    fn size_hint(&self) -> usize {
        match &self.0 {
            Ok(v) => Ok::<_, String>(v).size_hint(),
            Err(_) => 0,
        }
    }

    fn encode_to<T: parity_scale_codec::Output + ?Sized>(&self, dest: &mut T) {
        match &self.0 {
            Ok(t) => Ok::<_, String>(t).encode_to(dest),
            Err(e) => Err::<V, _>(e.to_string()).encode_to(dest),
        }
    }

    fn encode(&self) -> Vec<u8> {
        match &self.0 {
            Ok(t) => Ok::<_, String>(t).encode(),
            Err(e) => Err::<V, _>(e.to_string()).encode(),
        }
    }

    fn using_encoded<R, F: FnOnce(&[u8]) -> R>(&self, f: F) -> R {
        match &self.0 {
            Ok(t) => Ok::<_, String>(t).using_encoded(f),
            Err(e) => Err::<V, _>(e.to_string()).using_encoded(f),
        }
    }

    fn encoded_size(&self) -> usize {
        match &self.0 {
            Ok(t) => Ok::<_, String>(t).encoded_size(),
            Err(e) => Err::<V, _>(e.to_string()).encoded_size(),
        }
    }
}
