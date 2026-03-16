# Quick Start - Leaf Client Rust

## Build & Test

```bash
# Navigate to leaf workspace
cd /home/node/leaf

# Build the client
cargo build -p leaf-client-rust

# Run tests
cargo test -p leaf-client-rust

# Run demo
cargo run -p leaf-client-rust --example cbor_demo

# Run benchmarks
cargo bench -p leaf-client-rust
```

## Project Structure

```
leaf/clients/leaf-client-rust/
├── src/
│   ├── lib.rs      # Public API
│   ├── types.rs    # Protocol types (DID, queries, etc.)
│   ├── error.rs    # Error handling
│   └── codec.rs    # CBOR encode/decode
├── examples/
│   └── cbor_demo.rs  # Example usage
└── benches/
    └── cbor_bench.rs # Performance benchmarks
```

## Key Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/types.rs` | ~280 | All protocol type definitions |
| `src/codec.rs` | ~60 | CBOR encoding/decoding |
| `src/error.rs` | ~25 | Error types |
| `src/lib.rs` | ~20 | Public API exports |

## Status

- ✅ Phase 1: Core types & codec (COMPLETE)
- 🚧 Phase 2: Socket.IO client (TODO)
- 🚧 Phase 3: Testing & integration (TODO)
- 🚧 Phase 4: Performance comparison (TODO)

## Next Steps

See `PROGRESS.md` for detailed status and questions for the next session.
