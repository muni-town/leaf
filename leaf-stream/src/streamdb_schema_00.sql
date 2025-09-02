create table if not exists "events" (
    "id"        integer primary key,
    "user"      text not null,
    "payload"   blob not null
);

create table if not exists "stream_state" (
    "id"                    integer primary key,
    "stream_id"             blob not null,
    "module"                blob not null,
    "params"                blob not null,
    -- The latest event that has been processed by the module
    "module_event_cursor"   integer references events(id)
)
