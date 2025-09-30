-- Contains content-addressed WASM blobs
create table if not exists "wasm_blobs" (
    -- blake3 hash of the blob
    "hash"  blob unique not null primary key,
    -- the blob data
    "data"	blob not null,
    unique (hash, data)
);

-- Temporary staging table for WASM blobs that have been uploaded
-- but not used by a stream yet.
create table if not exists "staged_wasm" (
    -- The user that uploaded the staged wasm
    "creator"	blob not null,
    -- The hash of the WASM blob that was uploaded
    "hash"  blob not null references wasm_blobs(hash),
    -- The unix timestamp for when this WASM module was staged
    "timestamp" integer not null default (unixepoch())
);

-- The list of streams
create table if not exists "streams" (
    -- The stream ID
    "id"    blob not null primary key,
    -- The user that created the stream.
    "creator" text not null,
    -- The encoded genesis config of the stream.
    "genesis" blob not null,
    -- Current module
    "current_module" blob not null references wasm_blobs(hash),
    -- The index of the latest event in the stream
    "latest_event" integer,
    unique (id, creator)
);
