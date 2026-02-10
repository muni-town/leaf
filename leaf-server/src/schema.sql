-- Contains content-addressed WASM blobs
create table if not exists "module_blobs" (
    -- the blob's DASL content ID
    "cid"  blob unique not null primary key,
    -- the blob data
    "data"	blob not null,
    unique (cid, data)
) strict;


-- Temporary staging table for WASM blobs that have been uploaded
-- but not used by a stream yet.
create table if not exists "staged_modules" (
    -- The user that uploaded the staged wasm
    "creator"	text not null,
    -- The content ID of the WASM blob that was uploaded
    "cid"  blob not null references module_blobs(cid),
    -- The unix timestamp for when this WASM module was staged
    "timestamp" integer not null default (unixepoch())
) strict;

-- The list of DIDs that are used for streams on this server
create table if not exists "dids" (
    -- The DID
    "did" text unique not null primary key
) strict;

-- The list of private keys that we have for each DID on the server.
create table if not exists "did_keys" (
    -- The DID
    "did" text references dids(did),
    -- Binary p256 private key
    "p256_key" blob,
    -- Binary k256 private key
    "k256_key" blob
) strict;

-- The owners of the DIDs managed by this server
create table if not exists "did_owners" (
    -- The DID
    "did" text references dids(did),
    -- The user ID of the owner. This owner will be authorized to make modifications
    -- to the DID document if they successfully authenticate.
    "owner" text not null,
    unique (did, owner)
) strict;

-- The list of streams
create table if not exists "streams" (
    -- The stream ID
    "did"    text not null primary key references dids(did),
    -- ID of the WASM module needed by this stream, if any
    "module_cid" blob references module_blobs(cid),
    -- Optional client-side stamp sent when creating a stream
    "client_stamp" text,
    -- The index of the latest event in the stream
    "latest_event" integer not null
) strict;
