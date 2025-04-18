/**
 * @module @muni-town/leaf-storage-deno-fs
 */

import type { StorageInterface, StorageKey } from "@muni-town/leaf";
import { join, parse, relative } from "jsr:@std/path@1.0.8";
import { walk } from "jsr:@std/fs@1.0.10";

/**
 * Storage adapter that stores each snapshot in its own file on the filesystem.
 *
 * This is probably not the most efficient adapter when you have many small entities.
 */
export const denoFsStorageAdapter = (directory: string): StorageInterface => ({
  async load(key) {
    const path = join(directory, ...key);
    return await Deno.readFile(path);
  },
  async loadRange(key) {
    const path = join(directory, ...key);
    await Deno.mkdir(path, { recursive: true });
    const files: { key: StorageKey; data: Uint8Array | undefined }[] = [];
    for await (const entry of walk(path)) {
      if (entry.isFile) {
        files.push({
          key: relative(directory, entry.path)
            .split("/")
            .filter((x) => x != "."),
          data: await Deno.readFile(entry.path),
        });
      }
    }

    return files;
  },
  async remove(key) {
    const path = join(directory, ...key);
    await Deno.remove(path);
  },
  async removeRange(key) {
    const path = join(directory, ...key);
    await Deno.remove(path, { recursive: true });
  },
  async save(key, data) {
    const path = join(directory, ...key);
    const dir = parse(path).dir;
    await Deno.mkdir(dir, { recursive: true });
    await Deno.writeFile(path, data);
  },
});
