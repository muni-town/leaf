use parity_scale_codec::Encode;
use ulid::Ulid;

#[derive(Debug, Clone, Copy)]
pub struct Encodable<T>(pub T);
impl<T> From<T> for Encodable<T> {
    fn from(value: T) -> Self {
        Encodable(value)
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
