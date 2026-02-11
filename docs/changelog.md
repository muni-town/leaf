# Changelog

## Unreleased

- API (`stream/create`): Added optional `clientStamp` request/response field as advisory client metadata. `streamDid` remains the canonical, authoritative stream identifier and DID creation flow is unchanged.
- API (`stream/info`): Added optional `clientStamp` response field backed by persisted stream metadata.
- Storage (`streams`): Added nullable `client_stamp` column, migration support for existing databases, and stream info retrieval to preserve backward compatibility for legacy rows.
- Client compatibility: Added serialization compatibility tests so legacy request/response shapes continue to decode when new optional fields are present.
- Reliability (`stream/create`, `stream/subscribe_events`): Added per-actor throttling limits, active-subscription caps, and throttle reset/pruning tests to reduce abusive load without changing happy-path behavior.
- TypeScript client: `LeafClient.createStream` now optionally accepts `clientStamp` and returns it when present.
