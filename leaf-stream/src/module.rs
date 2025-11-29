#![allow(unused)] // Temporarily allow unused code since we are still working on this

use std::{
    char::decode_utf16,
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use blake3::Hash;
use futures::future::BoxFuture;
use leaf_stream_types::{
    BasicModuleDef, Decode, Encode, Event, IncomingEvent, LeafQuery, SqlRow, SqlRows, SqlValue,
};
use libsql::ScalarFunctionDef;
use regex::Regex;

use crate::scale::ScaleExtractExpr;

static SQL_COMMENT_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"--.*\n|/\*[^*]*\*+(?:[^/*][^*]*\*+)*/").unwrap());

// Module implementations
mod basic;
pub use basic::*;

/// A module that may be used to materialize a leaf stream.
pub trait LeafModule: Sync + Send {
    /// Get the module type.
    ///
    /// We may expand to support different types of modules on the Leaf server over time, or
    /// different servers may have different ones enabled.
    ///
    /// This describes the unique type name for the module that is being implemented by this
    /// implementation of the trait.
    ///
    /// These types are conventionally reverse domain IDs like town.muni.sql-wasm
    fn module_type_id() -> &'static str
    where
        Self: Sized;

    /// Load a module from it's serialized form
    fn load(bytes: LeafModuleCodec) -> anyhow::Result<Self>
    where
        Self: Sized;

    /// Save a module to it's serialized form
    fn save(&self) -> LeafModuleCodec;

    /// Get unique ID of the the loaded module.
    ///
    /// Note it is **required** that this match the ID returned by the [`LeafModuleCodec::id()`]
    /// that was used to load / save the module.
    fn module_id(&self) -> Hash;

    /// Setup the database connection when the module is first loaded.
    ///
    /// This gives you a chance to register any UDFs on the db connection if necessary.
    fn init_db_conn(&'_ self, module_db: &libsql::Connection) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Called to initialize the module database.
    ///
    /// > **Note:** It is **ilegal** to change the authorizer of the `module_db`. That will be
    /// > handled by the stream.
    fn init_db_schema(
        &'_ self,
        module_db: &libsql::Connection,
        creator: &str,
    ) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Called to materialize a new event
    ///
    /// > **Note:** It is **ilegal** to change the authorizer of the `module_db`. That will be
    /// > handled by the stream.
    fn materialize(
        &'_ self,
        module_db: &libsql::Connection,
        event: Event,
    ) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Called to authorize a new event
    ///
    /// > **Note:** It is **ilegal** to change the authorizer of the `module_db`. That will be
    /// > handled by the stream.
    fn authorize(
        &'_ self,
        module_db: &libsql::Connection,
        event: IncomingEvent,
    ) -> BoxFuture<'_, anyhow::Result<()>>;

    /// Called to query from the module
    ///
    /// > **Note:** It is **ilegal** to change the authorizer of the `module_db`. That will be
    /// > handled by the stream.
    fn query(
        &'_ self,
        module_db: &libsql::Connection,
        query: LeafQuery,
    ) -> BoxFuture<'_, anyhow::Result<SqlRows>>;
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct LeafModuleCodec {
    pub module_type_id: String,
    pub data: Vec<u8>,
}

impl LeafModuleCodec {
    fn id(&self) -> Hash {
        blake3::hash(&self.encode())
    }
}
