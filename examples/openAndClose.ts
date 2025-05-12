import { EntityId, EntityIdStr, Peer, StorageManager } from "@muni-town/leaf";
import { Age, Name } from "./components";
import { nodeFsStorageAdapter } from "@muni-town/leaf-storage-node-fs";

const peer = new Peer(
  new StorageManager(
    nodeFsStorageAdapter("./data/openAndClose")
  )
);

const id = new EntityId((process.argv[0] as EntityIdStr) || undefined);
console.log("ID", id.toString());

const ent = await peer.open(id);
console.log(ent.doc.toJSON());
ent.getOrInit(Name, (name) => name.set("first", "open"));
ent.getOrInit(Age, (age) => age.increment(1));
peer.close(ent);
