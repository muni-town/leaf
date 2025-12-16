#!/usr/bin/env node

import { parseArgs } from "node:util";
import { query } from "./commands/query.js";
import { sendEvents } from "./commands/send-events.js";
import { createStream } from "./commands/create-stream.js";
import { streamInfo } from "./commands/stream-info.js";

const HELP_TEXT = `
Leaf CLI - Testing tool for Leaf server

Usage:
  leaf <command> [options]

Commands:
  query <stream-id> <query-name>       Execute a SQL query on a stream
  send-events <stream-id> <file>       Send events to a stream from JSON file
  create-stream <genesis-file>         Create a new stream from genesis JSON
  stream-info <stream-id>              Get stream information

Global Options:
  --url <url>       Leaf server URL (default: http://localhost:5530 or LEAF_URL env var)
  --token <token>   Test auth token (default: test123 or LEAF_TEST_TOKEN env var)
  --help           Show this help message

Query Options:
  --start <n>      Start index for query (optional)
  --limit <n>      Limit number of results (optional)
  --params <json>  Query parameters as JSON object (optional)

Examples:
  leaf query abc123 events --start 0 --limit 1000
  leaf query abc123 custom --params '{"user":"did:plc:abc","count":5}'
  leaf send-events abc123 events.json
  leaf create-stream genesis.json
  leaf stream-info abc123

Environment Variables:
  LEAF_URL         Default Leaf server URL
  LEAF_TEST_TOKEN  Default test auth token
`;

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args[0] === "--help" || args[0] === "-h") {
    console.log(HELP_TEXT);
    process.exit(0);
  }

  const command = args[0];
  const commandArgs = args.slice(1);

  try {
    switch (command) {
      case "query":
        await query(commandArgs);
        break;
      case "send-events":
        await sendEvents(commandArgs);
        break;
      case "create-stream":
        await createStream(commandArgs);
        break;
      case "stream-info":
        await streamInfo(commandArgs);
        break;
      default:
        console.error(`Unknown command: ${command}`);
        console.error("Run 'leaf --help' for usage information");
        process.exit(1);
    }
  } catch (error) {
    console.error("Error:", error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

main();
