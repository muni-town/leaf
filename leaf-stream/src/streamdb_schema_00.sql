create table if not exists "events" (
    -- The event index, starting at 1
    "id"        integer primary key,
    -- The user that submitted the event
    "user"      text not null,
    -- The event payload
    "payload"   blob not null
);

create table if not exists "stream_state" (
    -- Primary key for this table, which is always 1
    "id"                    integer primary key check (id = 1),

    -- The ID of the user that created the stream
    "creator"               text not null,
    -- The ID of this stream
    "stream_id"             blob not null,
    -- The encoded definition of the stream's current module
    "module_def"             blob not null,
    -- The latest event that has been processed by the current module
    "module_event_cursor"   integer references events(id)
)
