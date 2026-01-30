use std::collections::HashMap;

use dasl::{
    cid::{Cid, Codec::Drisl},
    drisl,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleCodec {
    #[serde(rename = "$type")]
    module_type: String,
    #[serde(flatten)]
    def: drisl::Value,
}

#[derive(thiserror::Error, Debug)]
pub enum ModuleEncodingError {
    #[error("Error decoding DRISL format for module: {0}")]
    DrislEncode(String),
    #[error("Error decoding DRISL format for module: {0}")]
    DrislDecode(String),
    #[error("Module type does not match the type that we were attempting to load.")]
    ModuleTypeMismatch,
}

impl ModuleCodec {
    pub fn new<T: ModuleDef>(def: T) -> Result<Self, ModuleEncodingError> {
        let def = dasl::drisl::to_value(def)
            .map_err(|e| ModuleEncodingError::DrislEncode(e.to_string()))?;
        Ok(ModuleCodec {
            module_type: T::MODULE_TYPE.into(),
            def,
        })
    }

    pub fn module_type(&self) -> &str {
        &self.module_type
    }

    pub fn def(&self) -> &drisl::Value {
        &self.def
    }

    pub fn encode(&self) -> (Cid, Vec<u8>) {
        let bytes = dasl::drisl::to_vec(&self).expect(
            "error encoding module. It should not be possible \
            for ModuleCodec to fail encoding",
        );
        let cid = Cid::digest_sha2(Drisl, &bytes);
        (cid, bytes)
    }

    pub fn compute_id(&self) -> Cid {
        self.encode().0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ModuleEncodingError> {
        dasl::drisl::from_slice(bytes).map_err(|e| ModuleEncodingError::DrislDecode(e.to_string()))
    }

    pub fn decode_def<T: ModuleDef>(&self) -> Result<T, ModuleEncodingError> {
        if self.module_type != T::MODULE_TYPE {
            return Err(ModuleEncodingError::ModuleTypeMismatch);
        }
        let def = dasl::drisl::from_value(self.def.clone())
            .map_err(|e| ModuleEncodingError::DrislDecode(e.to_string()))?;
        Ok(def)
    }
}

pub trait ModuleDef: Serialize + for<'de> Deserialize<'de> {
    const MODULE_TYPE: &str;
}

/// The definition of a Leaf stream module.
///
/// Modules are responsible for providing the logic for writing to and querying from the stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BasicModuleDef {
    /// Idempodent initialization SQL that will be used to setup the module's SQLite database.
    ///
    /// This may be multiple statements separated by semicolons;
    ///
    /// The SQL should be idempotent as it may be executed multiple times.
    pub init_sql: String,

    /// SQL statements that will be used to authorize incoming events before they are inserted
    /// into the database.
    pub authorizer: String,

    /// SQL statements that will be used to materialize an event that has been authorized.
    pub materializer: String,

    /// SQL statements that will be used to materialize state events into the state db.
    pub state_materializer: String,

    /// Idempodent initialization SQL that will be used to setup the state database.
    ///
    /// This may be multiple statements separated by semicolons.
    ///
    /// This SQL is executed on the state database (attached as "state") after it is attached
    /// to the module database. It will be executed:
    /// - When the stream is first loaded
    /// - If the state database is reset.
    ///
    /// The SQL should be idempotent as it may be executed multiple times.
    #[serde(default)]
    pub state_init_sql: String,

    /// The list of queries defined by this module.
    ///
    /// Clients will be able to execute queries from this list by name.
    pub queries: Vec<LeafModuleQueryDef>,
}

impl ModuleDef for BasicModuleDef {
    const MODULE_TYPE: &str = "muni.town.leaf.module.basic.v0";
}

/// The definition of a query that can be made against a leaf module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafModuleQueryDef {
    /// The name of the query
    pub name: String,
    /// The SQL that will be executed to get the results of this query.
    pub sql: String,
    /// The list of parameter names, and the type for each parameter that may be passed into this
    /// query.
    pub params: Vec<LeafModuleQueryParamDef>,
}

// The definition of a queyr parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafModuleQueryParamDef {
    /// The name of the parameter. This will be accessible to the query's SQL as a bound
    /// parameter with the given name.
    pub name: String,
    /// The parameter's type. Parameters with invalid types will be rejected.
    pub kind: LeafModuleQueryParamKind,
    /// Whether or not this paramter can be omitted.
    pub optional: bool,
}

/// The type of a parameter that may be supplied to a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LeafModuleQueryParamKind {
    /// A 64 bit integer
    Integer,
    /// A 64 bit float
    Real,
    /// A string
    Text,
    /// A byte array
    Blob,
    /// Any of the above
    Any,
}

/// An event in a stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event<Payload = Vec<u8>> {
    pub idx: i64,
    pub user: String,
    pub payload: Payload,
    pub signature: Vec<u8>,
}

/// An incomming event that hasn't been accepted into a stream yet.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IncomingEvent<Payload = Vec<u8>> {
    pub user: String,
    pub payload: Payload,
}

/// A query for a leaf steram.
#[derive(Serialize, Deserialize, Debug, Clone /*, Hash, Eq, PartialEq*/)]
pub struct LeafQuery {
    pub name: String,
    pub params: HashMap<String, SqlValue>,
    pub start: Option<i64>,
    #[serde(default = "limit_default")]
    pub limit: i64,
}
fn limit_default() -> i64 {
    1000
}

impl LeafQuery {
    pub fn last_event(&self) -> i64 {
        self.limit + self.start.unwrap_or(1) - 1
    }

    /// Convert to a [`LeafQuery`] so that you can run it to get a single result instead of a
    /// subscription.
    pub fn update_for_subscription(&self, latest_event: i64) -> LeafQuery {
        LeafQuery {
            name: self.name.clone(),
            params: self.params.clone(),
            start: Some(
                self.start
                    .map(|x| x.max(latest_event))
                    .unwrap_or(latest_event),
            ),
            limit: self.limit,
        }
    }
}

pub type LeafQueryReponse = SqlRows;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LeafSubscribeEventsResponse {
    /// The rows returned by the subscritption
    pub rows: SqlRows,
    /// Indicates whether or not there will be another result immediately after this, for example, when backfilling.
    pub has_more: bool,
}

/// A result from a leaf query.
pub type SqlRows = Vec<SqlRow>;
pub type SqlRow = HashMap<String, SqlValue>;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "$type", content = "value")]
pub enum SqlValue {
    #[serde(rename = "muni.town.sqliteValue.null")]
    #[default]
    Null,
    #[serde(rename = "muni.town.sqliteValue.integer")]
    Integer(i64),
    #[serde(rename = "muni.town.sqliteValue.real")]
    Real(f64),
    #[serde(rename = "muni.town.sqliteValue.text")]
    Text(String),
    #[serde(rename = "muni.town.sqliteValue.blob")]
    #[serde(with = "dasl::drisl::serde_bytes")]
    Blob(Vec<u8>),
}

#[derive(Debug, thiserror::Error)]
pub enum QueryValidationError {
    #[error("The query name `{actual}` does not match the expected name: `{expected}`")]
    QueryNameDoesNotMatch { expected: String, actual: String },
    #[error("The `{param_name}` parameter does not exist in the `{query_name}` query.")]
    ParamDoesNotExist {
        query_name: String,
        param_name: String,
    },
    #[error(
        "The `{param_name}` parameter for the `{query_name}` query fails validation. \
        Got value `{param_value:?}` which needs to match param definition: {param_def:#?}."
    )]
    QueryParamFailsValidation {
        query_name: String,
        param_name: String,
        param_def: LeafModuleQueryParamDef,
        param_value: SqlValue,
    },
}

impl LeafModuleQueryDef {
    /// Returns a result indicating whether or not the provided query is valid for this query
    /// definition.
    pub fn validate_query(&self, query: &LeafQuery) -> Result<(), QueryValidationError> {
        // Validate the name matches
        if query.name != self.name {
            return Err(QueryValidationError::QueryNameDoesNotMatch {
                expected: self.name.clone(),
                actual: query.name.clone(),
            });
        }

        // Validate each param matches it's definition
        for (name, value) in &query.params {
            let param_def = self
                .params
                .iter()
                .find(|x| &x.name == name)
                .ok_or_else(|| QueryValidationError::ParamDoesNotExist {
                    query_name: self.name.clone(),
                    param_name: name.clone(),
                })?;
            let valid = param_def.value_is_valid(value);

            if !valid {
                return Err(QueryValidationError::QueryParamFailsValidation {
                    query_name: self.name.clone(),
                    param_name: name.clone(),
                    param_def: param_def.clone(),
                    param_value: value.clone(),
                });
            }
        }

        Ok(())
    }
}

impl LeafModuleQueryParamDef {
    pub fn value_is_valid(&self, value: &SqlValue) -> bool {
        use self::{LeafModuleQueryParamKind as K, SqlValue as V};
        match (self.optional, &self.kind, value) {
            (false, _, V::Null) => false, // Null provided for non-nullable value
            (true, _, V::Null) => true,   // Null provided for nullable value
            (_, K::Any, _) => true,       // Any value allowed
            (_, K::Integer, V::Integer(_)) => true,
            (_, K::Real, V::Real(_)) => true,
            (_, K::Text, V::Text(_)) => true,
            (_, K::Blob, V::Blob(_)) => true,
            _ => false,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SqlError {
    #[error("Invalid value type")]
    InvalidValueType,
    #[error("Invalid column count")]
    InvalidColumnCount,
}

pub type SqlResult<T> = Result<T, SqlError>;
