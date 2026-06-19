import {
  createClient,
  parseGlobalOptions,
  outputJson,
  outputError,
} from "../utils.js";

export async function listStreams(args: string[]) {
  const options = parseGlobalOptions(args);

  const client = await createClient(options);

  try {
    const streams = await client.listStreams();

    outputJson({
      success: true,
      streams,
    });
  } catch (error) {
    outputError(error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    client.disconnect();
  }
}
