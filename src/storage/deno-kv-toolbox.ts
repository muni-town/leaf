import { StorageInterface } from "../storage.ts";
import { get, set, remove, BLOB_META_KEY } from "@kitsonk/kv-toolbox/blob";
import { keys } from "@kitsonk/kv-toolbox/keys";
import { query } from "@kitsonk/kv-toolbox/query";

/**
 * Open a {@linkcode StorageInterface} that stores all data in a {@linkcode Deno.Kv} database.
 *
 * Unlike the non-"toolbox" Deno KV storage adapter, this one uses the Deno KV toolbox to chunk
 * blobs and allow you to store data larger than 64kb.
 *
 * @param kv The Deno key-value database.
 */
export function denoKvToolboxStorageAdapter(kv: Deno.Kv): StorageInterface {
  return {
    async load(key) {
      return (await get(kv, ["repo", ...key])).value || undefined;
    },
    async loadRange(prefix) {
      const chunks: Awaited<ReturnType<typeof this.loadRange>> = [];

      const rawKeys = await keys(query(kv, { prefix: ["repo", ...prefix] }));
      const blobKeys = rawKeys
        .map((x) =>
          x[x.length - 1] == BLOB_META_KEY
            ? x.slice(0, x.length - 1)
            : undefined
        )
        .filter((x) => !!x);

      for await (const key of blobKeys) {
        const data = (await get(kv, key)).value;
        if (!data) continue;
        chunks.push({
          key: key.slice(1) as string[],
          data,
        });
      }

      return chunks;
    },
    async remove(key) {
      await remove(kv, ["repo", ...key]);
    },
    async removeRange(prefix) {
      for await (const entry of kv.list({ prefix: ["repo", ...prefix] })) {
        await kv.delete(entry.key);
      }
    },
    async save(key, data) {
      await set(kv, ["repo", ...key], data);
    },
  };
}
