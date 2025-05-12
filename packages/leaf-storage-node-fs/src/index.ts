/**
 * @module @muni-town/leaf-storage-deno-fs
 */

import type { StorageInterface, StorageKey } from "@muni-town/leaf";
import { readFile, mkdir, opendir, rm, writeFile } from "node:fs/promises";
import { join, relative, parse } from "node:path";

async function* walk(dir: string): AsyncGenerator<string> {
  for await (const d of await opendir(dir)) {
    const entry = join(dir, d.name);
    if (d.isDirectory()) yield* walk(entry);
    else if (d.isFile()) yield entry;
  }
}

/**
 * Storage adapter that stores each snapshot in its own file on the filesystem.
 *
 * This is probably not the most efficient adapter when you have many small entities.
 */
export const nodeFsStorageAdapter = (directory: string): StorageInterface => ({
  async load(key) {
    const path = join(directory, ...key);
    return await readFile(path);
  },
  async loadRange(key) {
    const path = join(directory, ...key);
    await mkdir(path, { recursive: true });
    const files: { key: StorageKey; data: Uint8Array | undefined }[] = [];
    for await (const file of walk(path)) {
      files.push({
        key: relative(directory, file)
          .split("/")
          .filter((x) => x != "."),
        data: await readFile(file),
      });
    }

    return files;
  },
  async remove(key) {
    const path = join(directory, ...key);
    await rm(path);
  },
  async removeRange(key) {
    const path = join(directory, ...key);
    await rm(path, { recursive: true });
  },
  async save(key, data) {
    const path = join(directory, ...key);
    const dir = parse(path).dir;
    await mkdir(dir, { recursive: true });
    await writeFile(path, data);
  },
});
