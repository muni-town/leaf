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
    user_did TEXT NOT NULL PRIMARY KEY
) STRICT;

-- ----------------------------------------------------------------------------
-- Table: room_unreads
-- Purpose: Track unread counts per user per room within this space (stream)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS room_unreads (
    -- The DID of the user who has unreads
    user_did TEXT NOT NULL,
    -- The room ID (extracted from event payloads)
    room_id TEXT NOT NULL,
    -- Count of unread messages
    unread_count INTEGER NOT NULL DEFAULT 0,
    -- Count of mentions (messages where user was @mentioned)
    mention_count INTEGER NOT NULL DEFAULT 0,
    -- The last event index that was processed for this room
    last_event_idx INTEGER,

    PRIMARY KEY (user_did, room_id),
    FOREIGN KEY (user_did)
        REFERENCES space_members(user_did)
        ON DELETE CASCADE,
    CHECK (unread_count >= 0),
    CHECK (mention_count >= 0)
) STRICT;

-- Index for querying unreads for a user where unread_count > 0
-- This composite index efficiently supports queries filtering by both user_did and unread_count > 0
CREATE INDEX IF NOT EXISTS idx_room_unreads_user_unread
    ON room_unreads(user_did, unread_count DESC, room_id)
    WHERE unread_count > 0;

-- ----------------------------------------------------------------------------
-- Table: materialization_state
-- Purpose: Track the materialization progress for this stream
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS materialization_state (
    -- The last event index that was materialized
    last_event_idx INTEGER NOT NULL DEFAULT 0
) STRICT;
