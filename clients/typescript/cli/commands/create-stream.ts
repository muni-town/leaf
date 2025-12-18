import { readFile } from "node:fs/promises";
import {
  createClient,
  parseGlobalOptions,
  outputJson,
  outputError,
} from "../utils.js";

export async function createStream(args: string[]) {
  if (args.length < 1) {
    throw new Error("Usage: leaf create-stream <module-cid>");
  }

  const moduleCid = args[0]!;
  const options = parseGlobalOptions(args);

  const client = await createClient(options);

  try {
    const streamId = await client.createStream(moduleCid);

    outputJson({
      success: true,
      message: "Stream created successfully",
      stream_id: streamId,
    });
  } catch (error) {
    outputError(error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    client.disconnect();
  }
}
