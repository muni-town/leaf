import { Did } from "../../src/codec.js";
import {
  createClient,
  parseGlobalOptions,
  outputJson,
  outputError,
} from "../utils.js";

export async function streamInfo(args: string[]) {
  if (args.length < 1) {
    throw new Error("Usage: leaf stream-info <stream-id>");
  }

  const streamDid = args[0]! as Did;
  const options = parseGlobalOptions(args);

  const client = await createClient(options);

  try {
    console.error(`DEBUG: About to call client.streamInfo()`);
    console.error(`DEBUG: Socket connected: ${client.socket.connected}`);
    const info = await client.streamInfo(streamDid);
    console.error(`DEBUG: streamInfo returned successfully`);

    outputJson({
      success: true,
      stream_id: streamDid,
      info: {
        module_cid: info.module_cid,
      },
    });
  } catch (error) {
    console.error(`DEBUG: streamInfo threw error: ${error}`);
    outputError(error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    client.disconnect();
  }
}
