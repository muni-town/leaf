use ordered_float::OrderedFloat;
pub use parity_scale_codec::{Decode, Encode};

/// The definition of a Leaf stream module.
///
/// Modules are responsible for providing the logic for writing to and querying from the stream.
#[derive(Debug, Clone, Decode, Encode)]
pub struct LeafModuleDef {
    /// Idempodent initialization SQL that will be used to setup the module's SQLite database.
    ///
    /// This may be multiple statements separated by semicolons;
    pub init_sql: String,

    /// SQL statements that will be used to authorize incoming events before they are inserted
    /// into the database.
    pub authorizer: String,

    /// SQL statements that will be used to materialize an event that has been authorized.
    pub materializer: String,

    /// The list of queries defined by this module.
    ///
    /// Clients will be able to execute queries from this list by name.
    pub queries: Vec<LeafModuleQueryDef>,

    /// The hash of the WASM module that should be loaded along with this module to provide
    /// additional user-defined functions that can be called in the queries, authorizer, and
    /// materializer.
    pub wasm_module: Option<[u8; 32]>,
}

impl LeafModuleDef {
    /// Calculate the module ID and get the encoded bytes representation
    pub fn module_id_and_bytes(&self) -> (blake3::Hash, Vec<u8>) {
        let bytes = self.encode();
        let hash = blake3::hash(&bytes);
        (hash, bytes)
    }
}

/// The definition of a query that can be made against a leaf module.
#[derive(Debug, Clone, Decode, Encode)]
pub struct LeafModuleQueryDef {
    /// The name of the query
    pub name: String,
    /// The SQL that will be executed to get the results of this query.
    pub sql: String,
    /// The list of parameter names, and the type for each parameter that may be passed into this
    /// query.
    pub params: Vec<LeafModuleQueryParamDef>,
    /// List of optional limits on how this query may be called.
    pub limits: Vec<LeafModuleQueryDefLimit>,
}

// The definition of a queyr parameter
#[derive(Debug, Clone, Decode, Encode)]
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
#[derive(Debug, Clone, Decode, Encode)]
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

/// Defines a limit on how a query may be called.
///
/// We are unsure exactly how this will be done in the future but having this here allows us to add
/// limiting methods later.
///
/// For example, a limit might say that you may not subscribe to a query for realtime updates, or
/// that a query has a hard limit on the number of results that may be returned, or that the the
/// "cost" of the SQL query doesn't exceed a certain value, which could be important for DoS
/// prevention.
///
/// We currently don't have any kind of limits, but we may later, and having this here allows us to
/// add limits later in a backward compatible way.
#[derive(Debug, Clone, Decode, Encode)]
pub enum LeafModuleQueryDefLimit {}

/// An event in a stream.
#[derive(Clone, Debug, Encode, Decode)]
pub struct Event<Payload = Vec<u8>> {
    pub idx: i64,
    pub user: String,
    pub payload: Payload,
}

/// An incomming event that hasn't been accepted into a stream yet.
#[derive(Decode, Encode, Debug, Clone)]
pub struct IncomingEvent<Payload = Vec<u8>> {
    pub user: String,
    pub payload: Payload,
}

/// A query for a leaf steram.
#[derive(Decode, Encode, Debug, Clone, Hash, Eq, PartialEq)]
pub struct LeafQuery {
    pub query_name: String,
    pub requesting_user: String,
    pub params: Vec<(String, SqlValue)>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub limit: Option<i64>,
}

impl LeafQuery {
    pub fn last_event(&self) -> Option<i64> {
        self.limit.map(|l| l + self.start.unwrap_or(0))
    }
}

#[derive(Decode, Encode, Debug, Clone, Hash, Eq, PartialEq)]
pub struct LeafSubscribeQuery {
    pub query_name: String,
    pub requesting_user: String,
    pub params: Vec<(String, SqlValue)>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub batch_size: Option<i64>,
}

impl LeafSubscribeQuery {
    /// Convert to a [`LeafQuery`] so that you can run it to get a single result instead of a
    /// subscription.
    pub fn to_query(&self, latest_event: i64) -> LeafQuery {
        LeafQuery {
            query_name: self.query_name.clone(),
            requesting_user: self.requesting_user.clone(),
            params: self.params.clone(),
            start: Some(
                self.start
                    .map(|x| x.max(latest_event))
                    .unwrap_or(latest_event),
            ),
            end: self.end,
            limit: self.batch_size,
        }
    }
}

pub type LeafQueryReponse = SqlRows;

/// A result from a leaf query.
#[derive(Decode, Encode, Debug, Clone, Default)]
pub struct SqlRows {
    pub rows: Vec<SqlRow>,
    pub column_names: Vec<String>,
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub struct SqlRow {
    pub values: Vec<SqlValue>,
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub enum SqlValue {
    #[default]
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
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
        if query.query_name != self.name {
            return Err(QueryValidationError::QueryNameDoesNotMatch {
                expected: self.name.clone(),
                actual: query.query_name.clone(),
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

impl std::hash::Hash for SqlValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            SqlValue::Null => (),
            SqlValue::Integer(i) => i.hash(state),
            SqlValue::Real(r) => OrderedFloat(*r).hash(state),
            SqlValue::Text(t) => t.hash(state),
            SqlValue::Blob(b) => b.hash(state),
        }
    }
}
impl std::cmp::Eq for SqlValue {}
impl std::cmp::PartialEq for SqlValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::Real(l0), Self::Real(r0)) => OrderedFloat(*l0) == OrderedFloat(*r0),
            (Self::Text(l0), Self::Text(r0)) => l0 == r0,
            (Self::Blob(l0), Self::Blob(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl From<()> for SqlValue {
    fn from(_: ()) -> Self {
        SqlValue::Null
    }
}
impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}
impl From<f64> for SqlValue {
    fn from(value: f64) -> Self {
        SqlValue::Real(value)
    }
}
impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        SqlValue::Text(value)
    }
}
impl From<Vec<u8>> for SqlValue {
    fn from(value: Vec<u8>) -> Self {
        SqlValue::Blob(value)
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

pub trait FromValue: Sized {
    fn from_value(value: SqlValue) -> SqlResult<Self>;
}
pub trait FromRow: Sized {
    fn from_row(row: SqlRow) -> SqlResult<Self>;
}
pub trait FromRows: Sized {
    fn from_rows(rows: SqlRows) -> SqlResult<Self>;
}

impl SqlValue {
    pub fn parse_value<T: FromValue>(self) -> SqlResult<T> {
        T::from_value(self)
    }
}
impl SqlRow {
    pub fn parse_row<T: FromRow>(self) -> SqlResult<T> {
        T::from_row(self)
    }
}
impl SqlRows {
    pub fn parse_rows<T: FromRows>(self) -> SqlResult<T> {
        T::from_rows(self)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: SqlValue) -> SqlResult<Self> {
        match value {
            SqlValue::Null => Ok(None),
            v => Ok(Some(T::from_value(v)?)),
        }
    }
}

impl FromValue for [u8; 32] {
    fn from_value(value: SqlValue) -> SqlResult<Self> {
        match value {
            SqlValue::Blob(blob) => {
                let bytes: [u8; 32] = blob.try_into().map_err(|_| SqlError::InvalidValueType)?;
                Ok(bytes)
            }
            _ => Err(SqlError::InvalidValueType),
        }
    }
}

impl<T: FromRow> FromRows for Vec<T> {
    fn from_rows(rows: SqlRows) -> SqlResult<Self> {
        rows.rows
            .into_iter()
            .map(|row| T::from_row(row))
            .collect::<SqlResult<Vec<_>>>()
    }
}

impl FromValue for String {
    fn from_value(value: SqlValue) -> SqlResult<Self> {
        if let SqlValue::Text(x) = value {
            Ok(x)
        } else {
            Err(SqlError::InvalidValueType)
        }
    }
}
impl FromValue for i64 {
    fn from_value(value: SqlValue) -> SqlResult<Self> {
        if let SqlValue::Integer(x) = value {
            Ok(x)
        } else {
            Err(SqlError::InvalidValueType)
        }
    }
}
impl FromValue for f64 {
    fn from_value(value: SqlValue) -> SqlResult<Self> {
        if let SqlValue::Real(x) = value {
            Ok(x)
        } else {
            Err(SqlError::InvalidValueType)
        }
    }
}
impl FromValue for Vec<u8> {
    fn from_value(value: SqlValue) -> SqlResult<Self> {
        if let SqlValue::Blob(x) = value {
            Ok(x)
        } else {
            Err(SqlError::InvalidValueType)
        }
    }
}

impl FromValue for SqlValue {
    fn from_value(value: SqlValue) -> SqlResult<Self> {
        Ok(value)
    }
}

macro_rules! impl_from_row {
    ($($t:ident),*) => {
        impl<$($t: FromValue),*> FromRow for ($( $t ),*) {
            fn from_row(row: SqlRow) -> SqlResult<Self> {
                let mut values = row.values.into_iter();
                Ok((
                    $(
                        $t::from_value(values.next().ok_or(SqlError::InvalidColumnCount)?)?
                    ),*
                ))
            }
        }
    };
}
impl<T: FromValue> FromRow for T {
    fn from_row(row: SqlRow) -> SqlResult<Self> {
        T::from_value(
            row.values
                .into_iter()
                .next()
                .ok_or(SqlError::InvalidColumnCount)?,
        )
    }
}

impl<P: Decode + Encode> FromRow for Event<P> {
    fn from_row(row: SqlRow) -> SqlResult<Self> {
        let (idx, user, payload): (i64, String, Vec<u8>) = row.parse_row()?;

        Ok(Event {
            idx,
            user,
            payload: P::decode(&mut &payload[..]).map_err(|_e| SqlError::InvalidValueType)?,
        })
    }
}

impl_from_row!(A, B);
impl_from_row!(A, B, C);
impl_from_row!(A, B, C, D);
impl_from_row!(A, B, C, D, E);
impl_from_row!(A, B, C, D, E, F);
impl_from_row!(A, B, C, D, E, F, G);
