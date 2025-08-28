-- Contains content-addressed WASM blobs
create table if not exists "wasm_blobs" (
    "hash"  blob unique not null primary key,
    "data"	blob not null
);

-- Temporary staging table for WASM blobs that have been uploaded
-- but not used by a stream yet.
create table if not exists "staged_wasm" (
    "owner"	text not null,
    "hash"  text not null references wasm_blobs(hash)
);

create table if not exists "streams" (
    "id"    text not null,
    "owner" text not null
)

create table if not exists "streams_wasm_blobs" (
    "stream_id" text not null references streams(id),
    "blob_id"   
)