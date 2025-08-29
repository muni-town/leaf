use libsql::{Row, Rows, Value};

pub trait FromValue: Sized {
    async fn from_value(value: Value) -> anyhow::Result<Self>;
}
pub trait FromRow: Sized {
    async fn from_row(row: Row) -> anyhow::Result<Self>;
}
pub trait FromRows: Sized {
    async fn from_rows(rows: Rows) -> anyhow::Result<Self>;
}

pub struct StringOrBinaryAsHex(String);
impl std::fmt::Display for StringOrBinaryAsHex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::fmt::Debug for StringOrBinaryAsHex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"")?;
        f.write_str(&self.0)?;
        f.write_str("\"")
    }
}
impl FromValue for StringOrBinaryAsHex {
    async fn from_value(value: Value) -> anyhow::Result<Self> {
        match value {
            Value::Text(s) => Ok(StringOrBinaryAsHex(s)),
            Value::Blob(blob) => Ok(StringOrBinaryAsHex(hex::encode(blob))),
            _ => anyhow::bail!("Expected STRING or BLOB"),
        }
    }
}

impl<T: FromRow> FromRows for Vec<T> {
    async fn from_rows(mut rows: Rows) -> anyhow::Result<Self> {
        let mut v = Vec::new();
        while let Some(row) = rows.next().await? {
            v.push(T::from_row(row).await?);
        }
        Ok(v)
    }
}

impl FromValue for String {
    async fn from_value(value: Value) -> anyhow::Result<Self> {
        if let Value::Text(x) = value {
            Ok(x)
        } else {
            anyhow::bail!("Expected TEXT value")
        }
    }
}
impl FromValue for i64 {
    async fn from_value(value: Value) -> anyhow::Result<Self> {
        if let Value::Integer(x) = value {
            Ok(x)
        } else {
            anyhow::bail!("Expected INTEGER value")
        }
    }
}
impl FromValue for f64 {
    async fn from_value(value: Value) -> anyhow::Result<Self> {
        if let Value::Real(x) = value {
            Ok(x)
        } else {
            anyhow::bail!("Expected REAL value")
        }
    }
}
impl FromValue for Vec<u8> {
    async fn from_value(value: Value) -> anyhow::Result<Self> {
        if let Value::Blob(x) = value {
            Ok(x)
        } else {
            anyhow::bail!("Expected BLOB value")
        }
    }
}

impl FromValue for Value {
    async fn from_value(value: Value) -> anyhow::Result<Self> {
        Ok(value)
    }
}

macro_rules! impl_from_row {
    ($($t:ident),*) => {
        impl<$($t: FromValue),*> FromRow for ($( $t ),*) {
            async fn from_row(row: Row) -> anyhow::Result<Self> {
                let mut col = 0;
                Ok((
                    $(
                        #[allow(unused)]
                        {
                            let v = $t::from_value(row.get_value(col)?).await?;
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
    async fn from_row(row: Row) -> anyhow::Result<Self> {
        T::from_value(row.get_value(0)?).await
    }
}

impl_from_row!(A, B);
impl_from_row!(A, B, C);
impl_from_row!(A, B, C, D);
impl_from_row!(A, B, C, D, E);
impl_from_row!(A, B, C, D, E, F);
impl_from_row!(A, B, C, D, E, F, G);
