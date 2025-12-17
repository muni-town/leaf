import { readFile } from "node:fs/promises";
import {
  createClient,
  parseGlobalOptions,
  outputJson,
  outputError,
} from "../utils.js";
import type { Did, EventPayload } from "../../src/index.js";

export async function sendEvents(args: string[]) {
  if (args.length < 2) {
    throw new Error("Usage: leaf send-events <stream-id> <events-file>");
  }

  const streamDid = args[0]! as Did;
  const eventsFile = args[1]!;

  const options = parseGlobalOptions(args);

  // Read events from file
  let events: Uint8Array[];
  try {
    const fileContent = await readFile(eventsFile, "utf-8");
    const parsed = JSON.parse(fileContent);

    if (!Array.isArray(parsed)) {
      throw new Error("Events file must contain a JSON array");
    }

    events = parsed.map((event, index) => {
      if (!event.user || typeof event.user !== "string") {
        throw new Error(`Event ${index}: missing or invalid 'user' field`);
      }
      if (!event.payload || typeof event.payload !== "string") {
        throw new Error(
          `Event ${index}: missing or invalid 'payload' field (must be base64 string)`,
        );
      }

      // Convert base64 payload to Uint8Array
      const payload = Buffer.from(event.payload, "base64");

      return new Uint8Array(payload);
    });
  } catch (error) {
    if ((error as any).code === "ENOENT") {
      throw new Error(`Events file not found: ${eventsFile}`);
    }
    throw new Error(
      `Failed to parse events file: ${error instanceof Error ? error.message : String(error)}`,
    );
  }

  const client = await createClient(options);

  try {
    await client.sendEvents(streamDid, events);

    outputJson({
      success: true,
      message: `Successfully sent ${events.length} event(s) to stream ${streamDid}`,
      count: events.length,
    });
  } catch (error) {
    outputError(error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    client.disconnect();
  }
}
