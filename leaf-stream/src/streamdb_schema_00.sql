create table if not exists "events" (
    -- The event index, starting at 1
    "idx"        integer primary key,
    -- The user that submitted the event
    "user"      text not null,
    -- The event payload
    "payload"   blob not null,
    -- The signature for the event
    "signature" blob not null
);

create table if not exists "stream_state" (
    -- Primary key for this table, which is always 1
    "id"                    integer primary key check (id = 1),
    -- The ID of this stream
    "stream_did"             text not null,
    -- The hash of the stream's current module
    "module_cid"             blob,
    -- The latest event that has been processed by the current module
    "module_event_cursor"   integer references events(idx)
)
