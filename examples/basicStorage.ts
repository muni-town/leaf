import { Entity } from "../src/leaf.ts";
import { StorageManager } from "../src/storage.ts";
import { denoFsStorageAdapter } from "../src/storage/deno-fs.ts";
import { Age, Name } from "./components.ts";

const storage = new StorageManager(denoFsStorageAdapter("./data"));

const ent = new Entity(
  // By providing a specific entity ID we make sure that we will save and load the same doc across
  // runs of the script.
  "leaf:awg8ns27f5e164htq69r7tyb63vxsaj4ejpgqwa7e8gm00ppt5yg"
);

if (await storage.load(ent)) {
  console.log("Loaded Entity from storage.", ent.doc.toJSON());
} else {
  console.log("entity not in storage");
}

const name = ent.getOrInit(Name);
name.set("first", "Charlie");
name.set("last", "Browny");

// He ain't getting any younger
ent.getOrInit(Age).increment(1);

await storage.save(ent);

console.log("final", ent.doc.toJSON());
