import { assertEquals } from "jsr:@std/assert@1/equals";
import { EntityId, EntityIdStr, Peer } from "../src/index.ts";
import { StorageManager } from "../src/storage.ts";
import { denoKvStorageAdapter } from "../src/storage/deno-kv.ts";
import { Syncer1, SuperPeer1 } from "../src/sync1.ts";
import { Age } from "./components.ts";

/**
 * First we create a super peer to act as our "sync server". In this case it's local, but it would
 * be access over websockets or something similar most of the time.
 */
const superPeer = new SuperPeer1(
  new StorageManager(
    denoKvStorageAdapter(await Deno.openKv("./data/superPeer.sqlite"))
  )
);

/**
 * Now we create our two "local" peers that want to sync with each-other through the super peer.
 *
 * Super peer implements {@link Sync1Interface} so we can pass it into a syncer and use it as our
 * peers' syncer.
 */
const peer1 = new Peer(new Syncer1(superPeer));
const peer2 = new Peer(new Syncer1(superPeer));
const peer3 = new Peer(new Syncer1(superPeer));

const entityId = new EntityId((Deno.args[0] as EntityIdStr) || undefined);

const ent1 = await peer1.open(entityId);
const ent2 = await peer2.open(entityId);
const ent3 = await peer3.open(entityId);

ent1.getOrInit(Age).increment(1);
ent1.commit();

ent2.getOrInit(Age).increment(1);
ent2.commit();

ent3.getOrInit(Age).increment(1);
ent3.commit();

setTimeout(() => {
  console.log(`${ent1.id.toString()}`);
  console.log("Value1", ent1.doc.toJSON());
  console.log("Value2", ent2.doc.toJSON());
  console.log("Value3", ent3.doc.toJSON());
  assertEquals(ent1.doc.toJSON(), ent2.doc.toJSON());
  assertEquals(ent2.doc.toJSON(), ent3.doc.toJSON());
}, 100);
