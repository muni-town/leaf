import { EntityId, EntityIdStr, Peer } from "../src/leaf.ts";
import { StorageManager } from "../src/storage.ts";
import { denoKvStorageAdapter } from "../src/storage/deno-kv.ts";
import { Age, Name } from "./components.ts";

const peer = new Peer(
  new StorageManager(
    denoKvStorageAdapter(await Deno.openKv("./data/openAndClose.sqlite"))
  )
);

const id = new EntityId((Deno.args[0] as EntityIdStr) || undefined);
console.log("ID", id.toString());

const ent = await peer.open(id);
console.log(ent.doc.toJSON());
ent.getOrInit(Name).set("first", "open");
ent.getOrInit(Age).increment(1);
peer.close(ent);
