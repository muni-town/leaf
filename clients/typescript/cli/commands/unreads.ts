import { Did } from "../../src/codec.js";
import {
  createClient,
  parseGlobalOptions,
  outputJson,
  outputError,
} from "../utils.js";

export async function getUnreads(args: string[]) {
  if (args.length < 1) {
    throw new Error("Usage: leaf unreads <stream-did>");
  }

  const streamDid = args[0]! as Did;
  const options = parseGlobalOptions(args);

  const client = await createClient(options);

  try {
    const unreads = await client.getUnreads(streamDid);

    outputJson({
      success: true,
      stream_id: streamDid,
      unreads,
    });
  } catch (error) {
    outputError(error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    client.disconnect();
  }
}

export async function markAsRead(args: string[]) {
  if (args.length < 1) {
    throw new Error(
      "Usage: leaf mark-read <stream-did> [room-id] [last-read-idx]",
    );
  }

  const streamDid = args[0]! as Did;
  const roomId = args[1];
  const lastReadIdx = args[2] ? parseInt(args[2], 10) : undefined;
  const options = parseGlobalOptions(args);

  const client = await createClient(options);

  try {
    const success = await client.markAsRead(streamDid, roomId, lastReadIdx);

    outputJson({
      success,
      stream_id: streamDid,
      room_id: roomId,
      last_read_idx: lastReadIdx,
    });
  } catch (error) {
    outputError(error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    client.disconnect();
  }
}
