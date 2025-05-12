import assert from "node:assert";
import process from "node:process";
import {
  EntityIdStr,
  Peer,
  StorageManager,
  memorySync1Adapters,
  Syncer1,
} from "@muni-town/leaf";
import { nodeFsStorageAdapter } from "@muni-town/leaf-storage-node-fs";
import { Age, Name } from "./components";

// Create in-memory sync adapters
const [syncAdapter1, syncAdapter2] = memorySync1Adapters();

const peer1 = new Peer(
  new StorageManager(nodeFsStorageAdapter("data/peer1")),
  new Syncer1(syncAdapter1)
);
const peer2 = new Peer(
  new StorageManager(nodeFsStorageAdapter("data/peer2")),
  new Syncer1(syncAdapter2)
);

const id = (process.argv[2] as EntityIdStr) || undefined;
const ent1 = await peer1.open(id);
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
  assert.deepEqual(ent1.doc.toJSON(), ent2.doc.toJSON());
});
