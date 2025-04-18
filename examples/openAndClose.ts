import { EntityId, EntityIdStr, Peer, StorageManager } from "@muni-town/leaf";
import { denoKvBlobStorageAdapter } from "@muni-town/leaf-storage-deno-kv";
import { Age, Name } from "./components.ts";

const peer = new Peer(
  new StorageManager(
    denoKvBlobStorageAdapter(await Deno.openKv("./data/openAndClose.sqlite"))
  )
);

const id = new EntityId((Deno.args[0] as EntityIdStr) || undefined);
console.log("ID", id.toString());

const ent = await peer.open(id);
console.log(ent.doc.toJSON());
ent.getOrInit(Name, (name) => name.set("first", "open"));
ent.getOrInit(Age, (age) => age.increment(1));
peer.close(ent);
