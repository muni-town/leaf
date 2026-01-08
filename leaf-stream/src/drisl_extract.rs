#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum DrislExtractExprSegment {
    FieldAccess(String),
    ArrayAccess(usize),
    ExtractDiscriminant,
}

pub fn extract_sql_value_from_drisl(
    value: Value,
    expr: &str,
) -> Result<Option<libsql::Value>, anyhow::Error> {
    let expr = parse_drisl_extract_expr(expr)?;
    let value = extract_from_drisl_with_expr(value, &expr);
    Ok(value.map(drisl_to_sql))
}

pub fn drisl_to_sql(value: Value) -> libsql::Value {
    match value {
        Value::Integer(i) => libsql::Value::Integer(i as i64),
        Value::Bytes(bytes) => libsql::Value::Blob(bytes),
        Value::Float(f) => libsql::Value::Real(f),
        Value::Text(t) => libsql::Value::Text(t),
        Value::Bool(b) => libsql::Value::Integer(if b { 1 } else { 0 }),
        Value::Null => libsql::Value::Null,
        Value::Cid(cid) => libsql::Value::Blob(cid.as_bytes().to_vec()),
        value @ (Value::Map(_) | Value::Array(_)) => serde_json::to_string(&value)
            .map(libsql::Value::Text)
            .unwrap_or(libsql::Value::Null),
    }
}

pub fn extract_from_drisl_with_expr(
    value: Value,
    expr: &[DrislExtractExprSegment],
) -> Option<Value> {
    let (extractor, remaining_extractors) = match expr {
        [next, rest @ ..] => (next, rest),
        _ => return Some(value),
    };

    use DrislExtractExprSegment::*;
    use dasl::drisl::Value::*;
    let value = match (extractor, value) {
        (FieldAccess(f), Map(map)) => map.get(f).cloned(),
        (ArrayAccess(idx), Array(list)) => list.get(*idx).cloned(),
        (ExtractDiscriminant, Map(map)) => {
            if map.len() != 1 {
                None
            } else {
                Some(Text(map.keys().next().unwrap().clone()))
            }
        }
        (ExtractDiscriminant, discriminant @ Text(_)) => Some(discriminant),
        _ => None,
    }?;

    if !remaining_extractors.is_empty() {
        extract_from_drisl_with_expr(value, remaining_extractors)
    } else {
        Some(value)
    }
}

use dasl::drisl::Value;
pub use extract_expr_parser::expr as parse_drisl_extract_expr;

peg::parser! {
    grammar extract_expr_parser() for str {
        use DrislExtractExprSegment as Ex;

        // An expression is either...
        pub rule expr() -> Vec<Ex> = "." v:(segment() ** ".") { v }

        rule segment() -> Ex = discriminant() / array() / field()

        rule discriminant() -> Ex = "?discriminant" { Ex::ExtractDiscriminant }
        rule array() -> Ex  = n:$(['0'..='9']+) {? Ok(Ex::ArrayAccess(n.parse().or(Err("u32"))?)) }
        rule field() -> Ex = name:$(['$' | 'a'..='z' | 'A'..='Z']['0'..='9' | 'a'..='z' | 'A'..='Z']*) { Ex::FieldAccess(name.into()) }
    }
}

#[cfg(test)]
mod test {

    use dasl::drisl::to_value;
    use libsql::Value as V;
    use serde::Serialize;

    use crate::drisl_extract::extract_sql_value_from_drisl;

    #[derive(Serialize)]
    struct Example {
        name: String,
        age: u32,
        result: Result<Option<u16>, String>,
    }

    #[derive(Serialize)]
    enum E {
        Hello,
        World(String),
        N(u32),
    }

    #[derive(Serialize)]
    struct F {
        name: String,
        e: E,
        items: Vec<u32>,
    }

    #[test]
    fn extract_drisl() {
        let a = to_value(Example {
            name: "John".into(),
            age: 32,
            result: Ok(Some(7)),
        })
        .unwrap();
        let b = to_value(Example {
            name: "John".into(),
            age: 32,
            result: Ok(None),
        })
        .unwrap();
        let c = to_value(Example {
            name: "John".into(),
            age: 32,
            result: Err("error".into()),
        })
        .unwrap();
        let d = to_value(F {
            name: "test".into(),
            e: E::Hello,
            items: vec![1, 2, 3],
        })
        .unwrap();
        let e = to_value(F {
            name: "test".into(),
            e: E::N(77),
            items: vec![4, 5, 6],
        })
        .unwrap();
        let f = to_value(F {
            name: "test".into(),
            e: E::World("mary".into()),
            items: vec![7, 8, 9],
        })
        .unwrap();

        assert_eq!(
            extract_sql_value_from_drisl(a.clone(), ".name").unwrap(),
            Some(V::Text("John".into()))
        );
        assert_eq!(
            extract_sql_value_from_drisl(a.clone(), ".age").unwrap(),
            Some(V::Integer(32))
        );
        assert_eq!(
            extract_sql_value_from_drisl(a.clone(), ".result.Ok").unwrap(),
            Some(V::Integer(7))
        );
        assert_eq!(
            extract_sql_value_from_drisl(b.clone(), ".result.Ok").unwrap(),
            Some(V::Null)
        );
        assert_eq!(
            extract_sql_value_from_drisl(a.clone(), ".result.Err").unwrap(),
            None,
        );
        assert_eq!(
            extract_sql_value_from_drisl(b.clone(), ".result.Err").unwrap(),
            None,
        );
        assert_eq!(
            extract_sql_value_from_drisl(c.clone(), ".result.Err").unwrap(),
            Some(V::Text("error".into()))
        );
        assert_eq!(
            extract_sql_value_from_drisl(d.clone(), ".e").unwrap(),
            Some(V::Text("Hello".into()))
        );
        assert_eq!(
            extract_sql_value_from_drisl(d.clone(), ".e.?discriminant").unwrap(),
            Some(V::Text("Hello".into()))
        );
        assert_eq!(
            extract_sql_value_from_drisl(e.clone(), ".e.?discriminant").unwrap(),
            Some(V::Text("N".into()))
        );
        assert_eq!(
            extract_sql_value_from_drisl(e.clone(), ".e.N").unwrap(),
            Some(V::Integer(77))
        );
        assert_eq!(
            extract_sql_value_from_drisl(f.clone(), ".e.World").unwrap(),
            Some(V::Text("mary".into()))
        );
        assert_eq!(
            extract_sql_value_from_drisl(f.clone(), ".e.N").unwrap(),
            None,
        );
        assert_eq!(
            extract_sql_value_from_drisl(f.clone(), ".items.1").unwrap(),
            Some(V::Integer(8))
        );
    }
}
