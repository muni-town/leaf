# Leaf CLI

A command-line tool for testing and interacting with Leaf servers. This tool provides direct access to Leaf server operations, enabling stream inspection, event injection, and backfill testing.

## Installation

From the `leaf/clients/typescript` directory:

```bash
pnpm build
pnpm link --global
```

The `leaf` command will now be available globally, symlinked to the most recent build.

## Usage

```bash
leaf <command> [options]
```

### Global Options

All commands support these options:

- `--url <url>` - Leaf server URL (default: `http://localhost:5530` or `LEAF_URL` env var)
- `--token <token>` - Test auth token (default: `test123` or `LEAF_TEST_TOKEN` env var)
- `--help` - Show help message

### Environment Variables

- `LEAF_URL` - Default Leaf server URL
- `LEAF_TEST_TOKEN` - Default test authentication token

## Commands

### query

Execute SQL queries on a stream.

```bash
leaf query <stream-id> <query-name> [options]
```

**Options:**
- `--start <n>` - Start index for query (optional)
- `--limit <n>` - Limit number of results (optional)
- `--params <json>` - Query parameters as JSON object (optional)

**Examples:**

```bash
# Query events from a stream
leaf query abc123 events --start 0 --limit 1000

# Query with parameters
leaf query abc123 custom --params '{"user":"did:plc:abc","count":5}'

# Query with specific server
leaf query abc123 events --url http://localhost:5530 --token test123
```

**Output:**

```json
{
  "success": true,
  "rows": [
    {"id": 1, "user": "did:plc:...", "payload": "base64..."},
    {"id": 2, "user": "did:plc:...", "payload": "base64..."}
  ],
  "column_names": ["id", "user", "payload"],
  "count": 2
}
```

### send-events

Send events to a stream from a JSON file. This is useful for creating test fixtures by injecting events directly into the Leaf server.

```bash
leaf send-events <stream-id> <events-file>
```

**Events file format:**

```json
[
  {
    "user": "did:plc:abc123",
    "payload": "YmFzZTY0ZW5jb2RlZGRhdGE="
  },
  {
    "user": "did:plc:xyz789",
    "payload": "bW9yZWJhc2U2NGRhdGE="
  }
]
```

The `payload` field must be a base64-encoded string.

**Example:**

```bash
leaf send-events abc123 test-events.json
```

**Output:**

```json
{
  "success": true,
  "message": "Successfully sent 2 event(s) to stream abc123",
  "count": 2
}
```

### create-stream

Create a new stream from a genesis configuration file.

```bash
leaf create-stream <genesis-file>
```

**Genesis file format:**

```json
{
  "stamp": "01HK2QM8V7...",
  "creator": "did:plc:abc123",
  "module": "module-hash-here",
  "options": []
}
```

**Example:**

```bash
leaf create-stream genesis.json
```

**Output:**

```json
{
  "success": true,
  "message": "Stream created successfully",
  "stream_id": "abc123..."
}
```

### stream-info

Get metadata about a stream.

```bash
leaf stream-info <stream-id>
```

**Example:**

```bash
leaf stream-info abc123
```

**Output:**

```json
{
  "success": true,
  "stream_id": "abc123",
  "info": {
    "creator": "did:plc:abc123",
    "module_id": "module-hash"
  }
}
```

## Authentication

The CLI supports test authentication bypass for local development and testing:

1. **Test Token**: By default, the CLI uses the token `test123`. This is configured in the Leaf server for testing purposes.

2. **Custom Token**: Use `--token <your-token>` or set `LEAF_TEST_TOKEN` environment variable.

3. **How it works**: The token is sent to the Leaf server in place of an AT Protocol service auth JWT. The local Leaf server accepts this token and grants access without PDS validation.

**Note**: Test authentication only works with Leaf servers configured for testing. Production servers require proper AT Protocol authentication.

## Testing Use Cases

### Inspect Stream State

Query the current state of a stream directly on the Leaf server:

```bash
# Get all events
leaf query <stream-id> events --limit 10000

# Get specific range
leaf query <stream-id> events --start 1000 --limit 100
```

### Create Large Test Fixtures

Generate and inject thousands of events for backfill testing:

```bash
# Create events file with 5000 events
# (use a script to generate the JSON)

# Inject into stream
leaf send-events <stream-id> large-fixture.json

# Verify events were inserted
leaf query <stream-id> events --limit 5000
```

### Verify Backfill Correctness

1. Inject known events via CLI
2. Let Roomy client backfill the stream
3. Query the stream via CLI to verify all events were fetched
4. Compare client's materialized state with server state

## Error Handling

All commands output JSON on success and failure. Check the `success` field to determine the result:

**Success:**
```json
{
  "success": true,
  ...
}
```

**Failure:**
```json
{
  "success": false,
  "error": "Error message here"
}
```

Exit codes:
- `0` - Success
- `1` - Error (details in JSON output)

## Development

The CLI is built using the `@muni-town/leaf-client` TypeScript library.

**Structure:**
```
cli/
├── index.ts           # Entry point and command router
├── utils.ts           # Shared utilities (client creation, JSON output)
└── commands/
    ├── query.ts       # Query command implementation
    ├── send-events.ts # Send events command implementation
    ├── create-stream.ts # Create stream command implementation
    └── stream-info.ts # Stream info command implementation
```

**Building:**
```bash
pnpm build
```

The TypeScript compiler outputs to `dist/cli/`, and the `bin` entry in `package.json` points to `dist/cli/index.js`.

## Troubleshooting

**Command not found: leaf**

Run `pnpm link --global` from the `leaf/clients/typescript` directory.

**Connection timeout / Authentication timeout**

- Verify Leaf server is running at the specified URL
- Check that the server is configured to accept test tokens
- Ensure the URL is correct (default: `http://localhost:5530`)

**Socket disconnected errors**

- Check Leaf server logs for errors
- Verify the test token has proper permissions
- Ensure the stream ID is valid and exists

**Permission errors**

- The test token may not have access to the requested resource
- Check Leaf server configuration for test authentication setup
