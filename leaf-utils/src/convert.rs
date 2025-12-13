use std::{future::Future, str::FromStr};

use atproto_plc::Did;
use dasl::cid::Cid;
use libsql::{Row, Rows, Value};

pub trait FromValue: Sized {
    fn from_value(value: Value) -> libsql::Result<Self>;
}
pub trait FromRow: Sized {
    fn from_row(row: Row) -> impl Future<Output = libsql::Result<Self>>;
}
pub trait FromRows: Sized {
    fn from_rows(rows: Rows) -> impl Future<Output = libsql::Result<Self>>;
}

pub trait ParseValue<T>: Sized {
    fn parse_value(self) -> libsql::Result<T>;
}
impl<T: FromValue> ParseValue<T> for Value {
    fn parse_value(self) -> libsql::Result<T> {
        T::from_value(self)
    }
}
pub trait ParseRow<T>: Sized {
    fn parse_row(self) -> impl Future<Output = libsql::Result<T>>;
}
impl<T: FromRow> ParseRow<T> for Row {
    fn parse_row(self) -> impl Future<Output = libsql::Result<T>> {
        T::from_row(self)
    }
}

pub trait ParseRows<T>: Sized {
    fn parse_rows(self) -> impl Future<Output = libsql::Result<T>>;
}
impl<T: FromRows> ParseRows<T> for Rows {
    fn parse_rows(self) -> impl Future<Output = libsql::Result<T>> {
        T::from_rows(self)
    }
}

impl<T: FromValue> FromValue for Option<T> {
    fn from_value(value: Value) -> libsql::Result<Self> {
        match value {
            Value::Null => Ok(None),
            v => Ok(Some(T::from_value(v)?)),
        }
    }
}

impl FromValue for Did {
    fn from_value(value: Value) -> libsql::Result<Self> {
        match value {
            Value::Text(s) => Ok(Did::from_str(&s).map_err(|_| libsql::Error::InvalidColumnType)?),
            _ => Err(libsql::Error::InvalidColumnType),
        }
    }
}

impl FromValue for Cid {
    fn from_value(value: Value) -> libsql::Result<Self> {
        match value {
            Value::Text(s) => Ok(Cid::from_str(&s).map_err(|_| libsql::Error::InvalidColumnType)?),
            Value::Blob(blob) => {
                Ok(Cid::from_bytes(&blob).map_err(|_| libsql::Error::InvalidColumnType)?)
            }
            _ => Err(libsql::Error::InvalidColumnType),
        }
    }
}

impl<T: FromRow> FromRows for Vec<T> {
    async fn from_rows(mut rows: Rows) -> libsql::Result<Self> {
        let mut v = Vec::new();
        while let Some(row) = rows.next().await? {
            v.push(T::from_row(row).await?);
        }
        Ok(v)
    }
}

impl FromValue for String {
    fn from_value(value: Value) -> libsql::Result<Self> {
        if let Value::Text(x) = value {
            Ok(x)
        } else {
            Err(libsql::Error::InvalidColumnType)
        }
    }
}
impl FromValue for i64 {
    fn from_value(value: Value) -> libsql::Result<Self> {
        if let Value::Integer(x) = value {
            Ok(x)
        } else {
            Err(libsql::Error::InvalidColumnType)
        }
    }
}
impl FromValue for f64 {
    fn from_value(value: Value) -> libsql::Result<Self> {
        if let Value::Real(x) = value {
            Ok(x)
        } else {
            Err(libsql::Error::InvalidColumnType)
        }
    }
}
impl FromValue for Vec<u8> {
    fn from_value(value: Value) -> libsql::Result<Self> {
        if let Value::Blob(x) = value {
            Ok(x)
        } else {
            Err(libsql::Error::InvalidColumnType)
        }
    }
}

impl FromValue for Value {
    fn from_value(value: Value) -> libsql::Result<Self> {
        Ok(value)
    }
}

macro_rules! impl_from_row {
    ($($t:ident),*) => {
        impl<$($t: FromValue),*> FromRow for ($( $t ),*) {
            async fn from_row(row: Row) -> libsql::Result<Self> {
                let mut col = 0;
                Ok((
                    $(
                        #[allow(unused)]
                        {
                            let v = $t::from_value(row.get_value(col)?)?;
                            col += 1;
                            v
                        }
                    ),*
                ))
            }
        }
    };
}
impl<T: FromValue> FromRow for T {
    async fn from_row(row: Row) -> libsql::Result<Self> {
        T::from_value(row.get_value(0)?)
    }
}

impl_from_row!(A, B);
impl_from_row!(A, B, C);
impl_from_row!(A, B, C, D);
impl_from_row!(A, B, C, D, E);
impl_from_row!(A, B, C, D, E, F);
impl_from_row!(A, B, C, D, E, F, G);
