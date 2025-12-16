import { readFile } from "node:fs/promises";
import { createClient, parseGlobalOptions, outputJson, outputError } from "../utils.js";
import type { StreamGenesis } from "../../src/index.js";

export async function createStream(args: string[]) {
  if (args.length < 1) {
    throw new Error("Usage: leaf create-stream <genesis-file>");
  }

  const genesisFile = args[0]!;
  const options = parseGlobalOptions(args);

  // Read genesis from file
  let genesis: StreamGenesis;
  try {
    const fileContent = await readFile(genesisFile, "utf-8");
    const parsed = JSON.parse(fileContent);

    if (!parsed.stamp || !parsed.creator || !parsed.module) {
      throw new Error("Genesis file must contain 'stamp', 'creator', and 'module' fields");
    }

    genesis = {
      stamp: parsed.stamp,
      creator: parsed.creator,
      module: parsed.module,
      options: parsed.options || [],
    };
  } catch (error) {
    if ((error as any).code === "ENOENT") {
      throw new Error(`Genesis file not found: ${genesisFile}`);
    }
    throw new Error(`Failed to parse genesis file: ${error instanceof Error ? error.message : String(error)}`);
  }

  const client = await createClient(options);

  try {
    const streamId = await client.createStream(genesis);

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
