# Leaf Framework

This is the third major iteration of Muni Town's Leaf Framework. Under heavy development.

The Leaf server and client libraries are currently in a functional proof-of-concept state.

Meri's [notes from first reading the code](https://leaflet.pub/3abc7a5c-0790-4a4b-8ca1-a0a988bd7def) might be a little bit useful in lieu of more fleshed out documentation

To compile and run locally: `cargo r -- --otel  server -D did:web:localhost`. For testing, you can also pass `--unsafe-auth-token token123`, which enables an authentication bypass, so you don't need a JWT.


## Connection throttling for high-risk endpoints

`leaf-server` now applies lightweight per-actor throttling to these socket events:

- `stream/create`
- `stream/subscribe_events`

Actor identity is derived from authenticated DID when available. Anonymous connections (including local/dev sessions that skip JWTs) are grouped under a per-socket fallback actor key.

When running locally with `--unsafe-auth-token`, requests are attributed to the server DID, so throttling is still enforced and observable in logs/traces.

Self-hosters can tune the limits via CLI flags or env vars:

- `--throttle-window-secs` / `THROTTLE_WINDOW_SECS`
- `--stream-create-limit-per-window` / `STREAM_CREATE_LIMIT_PER_WINDOW`
- `--stream-subscribe-limit-per-window` / `STREAM_SUBSCRIBE_LIMIT_PER_WINDOW`
- `--max-active-subscriptions-per-actor` / `MAX_ACTIVE_SUBSCRIPTIONS_PER_ACTOR`

## Dockerfile build

The Dockerfile for `leaf-server` uses statically linked C code that is compiled for x86 architectures, which is also what the server is running. On x86 machines we can run `docker build -t leaf-server:x86_64 .`

For local dev on arm64 machines, we can pass in a build arg: `docker build --build-arg TARGET=aarch64-unknown-linux-musl -t leaf-server:arm64 .`

Then in either case we can run it as usual: `docker run -it --rm -p 5530:5530 -v $(pwd)/data:/data leaf-server`

## Protocol compatibility policy

For this iteration of the wire protocol, compatibility is maintained with additive changes only:

- Existing event names remain unchanged (for example `stream/create`, `stream/info`, and existing socket events).
- Existing required fields keep their current semantics (`streamDid`, `moduleCid`).
- Any newly introduced fields are additive and optional.
- No breaking wire changes are introduced in this iteration.
