pub use parity_scale_codec::{Decode, Encode};

#[derive(Decode, Encode, Debug, Clone)]
pub struct ModuleInit<Params = Vec<u8>> {
    pub creator: String,
    pub params: Params,
}

#[derive(Decode, Encode, Debug, Clone)]
pub struct IncomingEvent<Payload = Vec<u8>, Params = Vec<u8>> {
    pub payload: Payload,
    pub params: Params,
    pub user: String,
}

#[derive(Decode, Encode, Debug, Clone)]
pub struct EventRequest<Payload = Vec<u8>, Params = Vec<u8>> {
    pub requesting_user: String,
    pub incoming_event: IncomingEvent<Payload, Params>,
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub enum Inbound {
    #[default]
    Allow,
    Block {
        reason: String,
    },
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub enum Outbound {
    #[default]
    Allow,
    Block,
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub struct Process {
    pub new_module: Option<[u8; 32]>,
    pub new_params: Option<Vec<u8>>,
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub struct SqlQuery {
    pub sql: String,
    pub params: Vec<(String, SqlValue)>,
}

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

impl_from_row!(A, B);
impl_from_row!(A, B, C);
impl_from_row!(A, B, C, D);
impl_from_row!(A, B, C, D, E);
impl_from_row!(A, B, C, D, E, F);
impl_from_row!(A, B, C, D, E, F, G);
