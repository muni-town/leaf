import {
  createClient,
  parseGlobalOptions,
  getOption,
  outputJson,
  outputError,
} from "../utils.js";
import type { Did, LeafQuery } from "../../src/index.js";

export async function query(args: string[]) {
  if (args.length < 2) {
    throw new Error("Usage: leaf query <stream-did> <query-name> [options]");
  }

  const streamDid = args[0]! as Did;
  const queryName = args[1]!;

  const options = parseGlobalOptions(args);
  const startStr = getOption(args, "--start");
  const limitStr = getOption(args, "--limit");
  const paramsStr = getOption(args, "--params");

  // Build query object
  const leafQuery: LeafQuery = {
    name: queryName,
    params: [],
    start: startStr ? parseInt(startStr) : undefined,
    limit: limitStr ? parseInt(limitStr) : undefined,
  };

  // Parse params if provided
  if (paramsStr) {
    try {
      const paramsObj = JSON.parse(paramsStr);
      leafQuery.params = Object.entries(paramsObj).map(([key, value]) => {
        return [key, convertToSqlValue(value)];
      });
    } catch (e) {
      throw new Error(
        `Invalid params JSON: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
  }

  const client = await createClient(options);

  try {
    const result = await client.query(streamDid, leafQuery);

    // Convert SqlRows to a more readable format
    const rows = result.rows.map((row) => {
      const obj: Record<string, any> = {};
      result.column_names.forEach((name, index) => {
        const value = row[index];
        if (value) {
          obj[name] = convertFromSqlValue(value);
        }
      });
      return obj;
    });

    outputJson({
      success: true,
      rows,
      column_names: result.column_names,
      count: rows.length,
    });
  } catch (error) {
    outputError(error instanceof Error ? error.message : String(error));
    throw error;
  } finally {
    client.disconnect();
  }
}

// Helper to convert JS values to SQL values
function convertToSqlValue(value: any): any {
  if (value === null) {
    return { tag: "null" };
  } else if (typeof value === "number") {
    if (Number.isInteger(value)) {
      return { tag: "integer", value: BigInt(value) };
    } else {
      return { tag: "real", value };
    }
  } else if (typeof value === "string") {
    return { tag: "text", value };
  } else if (value instanceof Uint8Array) {
    return { tag: "blob", value };
  } else {
    throw new Error(`Unsupported value type: ${typeof value}`);
  }
}

// Helper to convert SQL values to JS values
function convertFromSqlValue(value: any): any {
  if (!value || typeof value !== "object" || !("tag" in value)) {
    return value;
  }

  switch (value.tag) {
    case "null":
      return null;
    case "integer":
      return Number(value.value);
    case "real":
      return value.value;
    case "text":
      return value.value;
    case "blob":
      // Convert Uint8Array to base64 for JSON output
      return Buffer.from(value.value).toString("base64");
    default:
      return value;
  }
}
