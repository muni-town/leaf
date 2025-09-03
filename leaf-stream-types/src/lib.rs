pub use parity_scale_codec::{Decode, Encode};

#[derive(Decode, Encode, Debug, Clone)]
pub struct ModuleInput<Payload = Vec<u8>, Params = Vec<u8>> {
    pub payload: Payload,
    pub params: Params,
    pub user: String,
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub enum InboundFilterResponse {
    #[default]
    Allow,
    Block {
        reason: String,
    },
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub enum OutboundFilterResponse {
    #[default]
    Allow,
    Block,
}

#[derive(Decode, Encode, Debug, Clone, Default)]
pub struct ModuleUpdate {
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

pub trait ParseValue<T>: Sized {
    fn parse_value(self) -> SqlResult<T>;
}
impl<T: FromValue> ParseValue<T> for SqlValue {
    fn parse_value(self) -> SqlResult<T> {
        T::from_value(self)
    }
}
pub trait ParseRow<T>: Sized {
    fn parse_row(self) -> SqlResult<T>;
}
impl<T: FromRow> ParseRow<T> for SqlRow {
    fn parse_row(self) -> SqlResult<T> {
        T::from_row(self)
    }
}

pub trait ParseRows<T>: Sized {
    fn parse_rows(self) -> SqlResult<T>;
}
impl<T: FromRows> ParseRows<T> for SqlRows {
    fn parse_rows(self) -> SqlResult<T> {
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
