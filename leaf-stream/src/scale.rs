use parity_scale_codec::{Decode, decode_vec_with_len};
use std::str::FromStr;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ScaleExtractExpr {
    Void,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    String,
    Bytes(Option<u32>),
    EnumDiscriminant(Vec<ScaleExtractExpr>),
    EnumValue(u8, Vec<ScaleExtractExpr>),
    Option(Box<ScaleExtractExpr>),
    ResultOk(Box<ScaleExtractExpr>),
    ResultErr(Box<ScaleExtractExpr>),
    Result(Box<ScaleExtractExpr>, Box<ScaleExtractExpr>),
    Chain(Vec<ScaleExtractExpr>),
}

impl ScaleExtractExpr {
    pub fn extract(&self, bytes: &mut &[u8]) -> Result<libsql::Value, parity_scale_codec::Error> {
        use libsql::Value as V;
        use parity_scale_codec::Input;
        let v = match self {
            Self::Void => V::Null,
            Self::U8 => V::Integer(u8::decode(bytes)? as i64),
            Self::I8 => V::Integer(i8::decode(bytes)? as i64),
            Self::U16 => V::Integer(u16::decode(bytes)? as i64),
            Self::I16 => V::Integer(i16::decode(bytes)? as i64),
            Self::U32 => V::Integer(u32::decode(bytes)? as i64),
            Self::I32 => V::Integer(i32::decode(bytes)? as i64),
            Self::U64 => V::Integer(u64::decode(bytes)? as i64),
            Self::I64 => V::Integer(i64::decode(bytes)?),
            Self::F32 => V::Real(f32::decode(bytes)? as f64),
            Self::F64 => V::Real(f64::decode(bytes)?),
            Self::String => V::Text(String::decode(bytes)?),
            Self::Bytes(len) => match len {
                Some(len) => V::Blob(decode_vec_with_len(bytes, *len as usize)?),
                None => V::Blob(Vec::<u8>::decode(bytes)?),
            },
            Self::Option(e) => match bytes.read_byte()? {
                0 => V::Null,
                1 => e.extract(bytes)?,
                _ => return Err("Invalid byte trying to read option".into()),
            },
            Self::ResultOk(e) => match bytes.read_byte()? {
                0 => e.extract(bytes)?,
                1 => V::Null,
                _ => return Err("Invalid byte trying to read result".into()),
            },
            Self::ResultErr(e) => match bytes.read_byte()? {
                0 => V::Null,
                1 => e.extract(bytes)?,
                _ => return Err("Invalid byte trying to read result".into()),
            },
            Self::Result(e1, e2) => match bytes.read_byte()? {
                0 => {
                    e1.extract(bytes)?;
                    V::Integer(1)
                }
                1 => {
                    e2.extract(bytes)?;
                    V::Integer(0)
                }
                _ => return Err("Invalid byte trying to read result".into()),
            },
            Self::Chain(exprs) => {
                let mut result = None;
                for e in exprs {
                    result = Some(e.extract(bytes)?);
                }
                result.unwrap_or(V::Null)
            }
            Self::EnumDiscriminant(variants) => {
                let discriminant = bytes.read_byte()?;
                let Some(variant) = variants.get(discriminant as usize) else {
                    return Err("Invalid byte trying to read enum discriminant".into());
                };
                variant.extract(bytes)?;
                V::Integer(discriminant as i64)
            }
            Self::EnumValue(requested_discriminant, variants) => {
                let discriminant = bytes.read_byte()?;
                let Some(variant) = variants.get(discriminant as usize) else {
                    return Err("Invalid byte trying to read enum discriminant".into());
                };
                let data = variant.extract(bytes)?;
                if discriminant == *requested_discriminant {
                    data
                } else {
                    V::Null
                }
            }
        };
        Ok(v)
    }
}

impl FromStr for ScaleExtractExpr {
    type Err = peg::error::ParseError<peg::str::LineCol>;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        extract_expr_parser::expr(s)
    }
}

peg::parser! {
    grammar extract_expr_parser() for str {
        use ScaleExtractExpr as Ex;

        // An expression is either...
        pub rule expr() -> Ex =
            // a chain of single expressions or...
            e:(single_expr() **<1,> "|") {
                if e.len() > 1 {
                    Ex::Chain(e)
                } else {
                    e.into_iter().next().unwrap()
                }
            } /
            // a single expression.
            e:single_expr() { e }

        rule single_expr() -> Ex =
            "void" { Ex::Void } /
            "u8" { Ex::U8 } /
            "i8" { Ex::I8 } /
            "u16" { Ex::U16 } /
            "i16" { Ex::I16 } /
            "u32" { Ex::U32 } /
            "i32" { Ex::I32 } /
            "u64" { Ex::U64 } /
            "i64" { Ex::I64 } /
            "f32" { Ex::F32 } /
            "f64" { Ex::F64 } /
            "string" { Ex::String } /
            "bytes" { Ex::Bytes(None) } /
            "bytes(" len:bytes_len() ")" { Ex::Bytes(Some(len)) } /
            "option(" e:expr() ")" { Ex::Option(Box::new(e)) } /
            "result_err(" e:expr() ")" { Ex::ResultErr(Box::new(e)) } /
            "result_ok(" e:expr() ")" { Ex::ResultOk(Box::new(e)) } /
            "result(" e1:expr() "," e2:expr() ")" { Ex::Result(Box::new(e1), Box::new(e2)) } /
            "enum(" discriminant:discriminant() ","  variants:(single_expr() ++ ",") ")" { Ex::EnumValue(discriminant, variants) } /
            "enum_discriminant(" variants:(single_expr() ++ ",") ")" { Ex::EnumDiscriminant(variants) }

        rule bytes_len() -> u32 = n:$(['0'..='9']+) {? n.parse().or(Err("invalid u32 for bytes len")) }
        rule discriminant() -> u8 = n:$(['0'..='9']+) {? n.parse().or(Err("invalid u8 for enum discriminant")) }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use libsql::Value as V;
    use parity_scale_codec::Encode;

    use crate::scale::ScaleExtractExpr;

    #[derive(Encode)]
    struct Example {
        name: String,
        age: u32,
        result: Result<Option<u16>, String>,
    }

    #[derive(Encode)]
    enum E {
        Hello,
        World(String),
        N(u32),
    }

    #[derive(Encode)]
    struct F {
        name: String,
        e: E,
    }

    #[test]
    fn extract_scale() {
        use super::ScaleExtractExpr::*;

        let a = Example {
            name: "John".into(),
            age: 32,
            result: Ok(Some(7)),
        }
        .encode();
        let b = Example {
            name: "John".into(),
            age: 32,
            result: Ok(None),
        }
        .encode();
        let c = Example {
            name: "John".into(),
            age: 32,
            result: Err("error".into()),
        }
        .encode();
        let d = F {
            name: "test".into(),
            e: E::Hello,
        }
        .encode();
        let e = F {
            name: "test".into(),
            e: E::N(77),
        }
        .encode();
        let f = F {
            name: "test".into(),
            e: E::World("mary".into()),
        }
        .encode();

        assert_eq!(String.extract(&mut &a[..]).unwrap(), V::Text("John".into()));
        assert_eq!(
            Chain(vec![String, U32]).extract(&mut &a[..]).unwrap(),
            V::Integer(32)
        );
        assert_eq!(
            Chain(vec![String, U32, ResultOk(Box::new(Option(Box::new(U16))))])
                .extract(&mut &a[..])
                .unwrap(),
            V::Integer(7)
        );
        assert_eq!(
            Chain(vec![String, U32, ResultErr(Box::new(String))])
                .extract(&mut &a[..])
                .unwrap(),
            V::Null
        );
        assert_eq!(
            Chain(vec![String, U32, ResultErr(Box::new(String))])
                .extract(&mut &b[..])
                .unwrap(),
            V::Null
        );
        assert_eq!(
            Chain(vec![String, U32, ResultErr(Box::new(String))])
                .extract(&mut &c[..])
                .unwrap(),
            V::Text("error".into())
        );
        assert_eq!(
            ScaleExtractExpr::from_str("string|enum_discriminant(void,string,u32)")
                .unwrap()
                .extract(&mut &d[..])
                .unwrap(),
            V::Integer(0)
        );
        assert_eq!(
            ScaleExtractExpr::from_str("string|enum_discriminant(void,string,u32)")
                .unwrap()
                .extract(&mut &e[..])
                .unwrap(),
            V::Integer(2)
        );
        assert_eq!(
            ScaleExtractExpr::from_str("string|enum(2,void,string,u32)")
                .unwrap()
                .extract(&mut &e[..])
                .unwrap(),
            V::Integer(77)
        );
        assert_eq!(
            ScaleExtractExpr::from_str("string|enum(1,void,string,u32)")
                .unwrap()
                .extract(&mut &f[..])
                .unwrap(),
            V::Text("mary".into())
        );
        assert_eq!(
            ScaleExtractExpr::from_str("string|enum(2,void,string,u32)")
                .unwrap()
                .extract(&mut &f[..])
                .unwrap(),
            V::Null
        );
    }

    #[test]
    fn parse_scale_extract_expr() {
        use super::ScaleExtractExpr::*;
        let cases = vec![
            ("u64", U64),
            ("f32", F32),
            ("f32|u32", Chain(vec![F32, U32])),
            ("f32|u32|bytes", Chain(vec![F32, U32, Bytes(None)])),
            (
                "f32|u32|bytes|option(f32|u32)",
                Chain(vec![
                    F32,
                    U32,
                    Bytes(None),
                    Option(Box::new(Chain(vec![F32, U32]))),
                ]),
            ),
            (
                "result_ok(f32|u32)",
                ResultOk(Box::new(Chain(vec![F32, U32]))),
            ),
            ("result(f32,u32)", Result(Box::new(F32), Box::new(U32))),
            (
                "result_ok(f32|option(u32|i8))",
                ResultOk(Box::new(Chain(vec![
                    F32,
                    Option(Box::new(Chain(vec![U32, I8]))),
                ]))),
            ),
        ];

        for (str, expected) in cases {
            match str.parse::<super::ScaleExtractExpr>() {
                Ok(v) => assert_eq!(v, expected),
                Err(e) => {
                    panic!("Could not parse expression `{str}`: {e}");
                }
            }
        }
    }
}
