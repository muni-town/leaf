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
    "owner"	blob not null,
    -- The hash of the WASM blob that was uploaded
    "hash"  blob not null references wasm_blobs(hash),
    -- The unix timestamp for when this WASM module was staged
    "timestamp" integer not null default (unixepoch())
);

-- The list of streams
create table if not exists "streams" (
    -- The stream ID
    "id"    blob not null primary key,
    -- The user that created the stream
    "owner" text not null,
    unique (id, owner)
);

-- table that ties streams to the blobs that they depend on.
create table if not exists "streams_wasm_blobs" (
    "stream_id" blob not null references streams(id) on delete cascade,
    "blob_hash" blob not null references wasm_blobs(hash),
    unique (stream_id, blob_hash)
);