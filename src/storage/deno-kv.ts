import { StorageInterface } from "../storage.ts";

/**
 * Open a {@linkcode StorageInterface} that stores all data in a {@linkcode Deno.Kv} database.
 *
 * @param kv The Deno key-value database.
 */
export function denoKvStorageAdapter(kv: Deno.Kv): StorageInterface {
  return {
    async load(key) {
      return (await kv.get<Uint8Array>(["repo", ...key])).value || undefined;
    },
    async loadRange(prefix) {
      const chunks: Awaited<ReturnType<typeof this.loadRange>> = [];
      for await (const entry of kv.list({ prefix: ["repo", ...prefix] })) {
        chunks.push({
          key: entry.key.slice(1) as string[],
          data: entry.value as Uint8Array | undefined,
        });
      }
      return chunks;
    },
    async remove(key) {
      await kv.delete(["repo", ...key]);
    },
    async removeRange(prefix) {
      for await (const entry of kv.list({ prefix: ["repo", ...prefix] })) {
        await kv.delete(entry.key);
      }
    },
    async save(key, data) {
      await kv.set(["repo", ...key], data);
    },
  };
}
