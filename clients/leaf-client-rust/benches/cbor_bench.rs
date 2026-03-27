//! Benchmark for CBOR encoding/decoding
//!
//! Run with: cargo bench -p leaf-client-rust

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use leaf_client_rust::{codec, types::*};
use std::collections::HashMap;

fn bench_encode_decode_sql_value(c: &mut Criterion) {
    let value = SqlValue::Integer { value: 42 };

    let mut group = c.benchmark_group("cbor/sql_value");
    group.bench_function("encode", |b| {
        b.iter(|| codec::encode(black_box(&value)))
    });
    group.bench_function("decode", |b| {
        let encoded = codec::encode(&value).unwrap();
        b.iter(|| codec::decode::<SqlValue>(black_box(&encoded)))
    });
    group.finish();
}

fn bench_encode_decode_query(c: &mut Criterion) {
    let mut params = HashMap::new();
    params.insert(
        "user_id".to_string(),
        SqlValueRaw::Text {
            value: "did:web:example.com".to_string(),
        },
    );
    params.insert("limit".to_string(), SqlValueRaw::Integer { value: 10 });

    let query = LeafQuery {
        name: "get_messages".to_string(),
        params,
        start: Some(0),
        limit: Some(50),
    };

    let mut group = c.benchmark_group("cbor/query");
    group.bench_function("encode", |b| {
        b.iter(|| codec::encode(black_box(&query)))
    });
    group.bench_function("decode", |b| {
        let encoded = codec::encode(&query).unwrap();
        b.iter(|| codec::decode::<LeafQuery>(black_box(&encoded)))
    });
    group.finish();
}

fn bench_large_query(c: &mut Criterion) {
    let mut params = HashMap::new();
    for i in 0..10 {
        params.insert(
            format!("param_{}", i),
            SqlValueRaw::Text {
                value: format!("value_{}", i),
            },
        );
    }

    let query = LeafQuery {
        name: "complex_query".to_string(),
        params,
        start: Some(0),
        limit: Some(1000),
    };

    let mut group = c.benchmark_group("cbor/large_query");
    group.bench_function("encode", |b| {
        b.iter(|| codec::encode(black_box(&query)))
    });
    group.bench_function("decode", |b| {
        let encoded = codec::encode(&query).unwrap();
        b.iter(|| codec::decode::<LeafQuery>(black_box(&encoded)))
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_encode_decode_sql_value,
    bench_encode_decode_query,
    bench_large_query
);
criterion_main!(benches);
