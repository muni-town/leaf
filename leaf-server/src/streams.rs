use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use blake3::Hash;
use libsql::Connection;
use parity_scale_codec::Encode;
use wasmer::Module;

use crate::serde::{SerdeBinaryBase64, SerdeRawHash, SerdeUlid};

pub static STREAMS: LazyLock<Streams> = LazyLock::new(Streams::default);

#[derive(Default)]
pub struct Streams {
    pub streams: HashMap<Hash, OpenStream>,
}

/// A stream that has been opened in memory.
pub struct OpenStream {
    /// Connection to the stream's event database.
    pub event_db: Connection,
    /// Connection to the filter module's database.
    pub module_db: Connection,
    /// Parameters to the module.
    pub params: Vec<u8>,
    /// The WASM filter module.
    pub module: Arc<Module>,
}
