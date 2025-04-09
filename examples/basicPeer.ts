import { assertEquals } from "jsr:@std/assert@1/equals";
import { EntityIdStr, Peer } from "../src/index.ts";
import { StorageManager } from "../src/storage.ts";
import { denoKvToolboxStorageAdapter } from "../src/storage/deno-kv-toolbox.ts";
import { memorySync1Adapters, Syncer1 } from "../src/sync1.ts";
import { Age, Name } from "./components.ts";

// Create in-memory sync adapters
const [syncAdapter1, syncAdapter2] = memorySync1Adapters();

const peer1 = new Peer(
  new StorageManager(
    denoKvToolboxStorageAdapter(await Deno.openKv("data/peer1.sqlite"))
  ),
  new Syncer1(syncAdapter1)
);
const peer2 = new Peer(
  new StorageManager(
    denoKvToolboxStorageAdapter(await Deno.openKv("data/peer2.sqlite"))
  ),
  new Syncer1(syncAdapter2)
);

const ent1 = await peer1.open((Deno.args[0] as EntityIdStr) || undefined);
console.log("ID", ent1.id.toString());
console.log("Value1", ent1.doc.toJSON());

const ent2 = await peer2.open(ent1.id);
console.log("Value2", ent2.doc.toJSON());

ent1.getOrInit(Name, (name) => name.set("first", "John"));
// Don't forget to commit if you want to force changes to sync.
ent1.commit();

ent2.getOrInit(Age, (age) => age.increment(1));
ent2.commit();

setTimeout(() => {
  console.log("====");
  console.log("Value1", ent1.doc.toJSON());
  console.log("Value2", ent2.doc.toJSON());
  assertEquals(ent1.doc.toJSON(), ent2.doc.toJSON());
});
