import { StorageInterface, StorageKey } from "../storage.ts";
import { join, parse, relative } from "jsr:@std/path";
import { walk } from "jsr:@std/fs";

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
