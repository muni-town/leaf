import { LeafClient } from "../src/index.js";

export interface GlobalOptions {
  url: string;
  token: string;
}

export function parseGlobalOptions(args: string[]): GlobalOptions {
  const url = getOption(args, "--url") || process.env.LEAF_URL || "http://localhost:5530";
  const token = getOption(args, "--token") || process.env.LEAF_TEST_TOKEN || "test123";

  return { url, token };
}

export function getOption(args: string[], flag: string): string | undefined {
  const index = args.indexOf(flag);
  if (index !== -1 && index + 1 < args.length) {
    return args[index + 1];
  }
  return undefined;
}

export function hasFlag(args: string[], flag: string): boolean {
  return args.includes(flag);
}

export async function createClient(options: GlobalOptions): Promise<LeafClient> {
  const client = new LeafClient(options.url, async () => options.token);

  // Listen for native socket.io errors
  client.socket.on("connect_error", (err) => {
    console.error(`DEBUG: connect_error: ${err.message}`);
  });

  // Wait for authentication to complete
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Authentication timeout"));
    }, 5000);

    let authenticated = false;

    client.on("connect", () => {
      console.error("DEBUG: Socket connected");
    });

    client.on("authenticated", (did) => {
      console.error(`DEBUG: Authenticated with DID: ${did}`);
      authenticated = true;
      clearTimeout(timeout);
      resolve();
    });

    client.on("disconnect", () => {
      console.error("DEBUG: Socket disconnected");
      if (!authenticated) {
        clearTimeout(timeout);
        reject(new Error("socket disconnected before authentication completed"));
      }
    });

    client.on("error", (error) => {
      console.error(`DEBUG: Socket error: ${error}`);
      clearTimeout(timeout);
      reject(new Error(`Connection error: ${error}`));
    });
  });

  return client;
}

export function outputJson(data: any) {
  console.log(JSON.stringify(data, null, 2));
}

export function outputError(message: string) {
  console.error(JSON.stringify({ success: false, error: message }, null, 2));
}
