-- ============================================================================
-- UNREADS DATABASE SCHEMA
-- Location: {data_dir}/streams/{stream_did}/unreads.db
-- Purpose: Track unread counts per user per room and space membership for a single stream
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Table: space_members
-- Purpose: Track which users are members of this space (stream)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS space_members (
    -- The DID of the user who is a member
    user_did TEXT NOT NULL PRIMARY KEY,
    -- When the user joined the space
    joined_at INTEGER NOT NULL DEFAULT (unixepoch()),
    -- When the user left the space (NULL if still a member)
    left_at INTEGER,
    -- The event index that caused this membership change
    event_idx INTEGER,

    CHECK (left_at IS NULL OR left_at >= joined_at)
) STRICT;

-- Index for querying active members
CREATE INDEX IF NOT EXISTS idx_space_members_active
    ON space_members(user_did)
    WHERE left_at IS NULL;

-- ----------------------------------------------------------------------------
-- Table: room_unreads
-- Purpose: Track unread counts per user per room within this space (stream)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS room_unreads (
    -- The room ID (extracted from event payloads)
    room_id TEXT NOT NULL,
    -- The DID of the user who has unreads
    user_did TEXT NOT NULL,
    -- Count of unread messages
    unread_count INTEGER NOT NULL DEFAULT 0,
    -- Count of mentions (messages where user was @mentioned)
    mention_count INTEGER NOT NULL DEFAULT 0,
    -- The last event index that was processed for this room
    last_event_idx INTEGER,
    -- Timestamp of last update
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),

    PRIMARY KEY (room_id, user_did),
    FOREIGN KEY (user_did)
        REFERENCES space_members(user_did)
        ON DELETE CASCADE,
    CHECK (unread_count >= 0),
    CHECK (mention_count >= 0)
) STRICT;

-- Index for querying unreads for a user across all rooms
CREATE INDEX IF NOT EXISTS idx_room_unreads_user
    ON room_unreads(user_did, unread_count DESC, mention_count DESC);

-- Index for querying unreads in a specific room
CREATE INDEX IF NOT EXISTS idx_room_unreads_room
    ON room_unreads(room_id)
    WHERE unread_count > 0 OR mention_count > 0;

-- ----------------------------------------------------------------------------
-- Table: materialization_state
-- Purpose: Track the materialization progress for this stream
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS materialization_state (
    -- The last event index that was materialized
    last_event_idx INTEGER NOT NULL DEFAULT 0,
    -- Timestamp of last successful materialization
    last_materialized_at INTEGER NOT NULL DEFAULT (unixepoch())
) STRICT;
