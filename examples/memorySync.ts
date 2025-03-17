import { Entity } from "../src/index.ts";
import { memorySync1Adapters, Syncer1 } from "../src/sync1.ts";
import { Age, Name } from "./components.ts";
import { assertEquals } from "jsr:@std/assert@1/equals";

// NOTE: usually you'll use a Peer to do this instead of manually using syncers.

// Create in-memory sync adapters
const [syncAdapter1, syncAdapter2] = memorySync1Adapters();

// Create a syncer for each adapter
const sync1 = new Syncer1(syncAdapter1);
const sync2 = new Syncer1(syncAdapter2);

// Create a new entity
const ent1 = new Entity();
// Have the first syncer sync it.
sync1.sync(ent1);

// Create another entity with the same ID
const ent2 = new Entity(ent1.id);
// Have the second syncer sync it
sync2.sync(ent2);

// Now ent1 and ent2 should stay in sync with each-other.
const log = () => {
  console.log("====");
  console.log("ent1", ent1.doc.toJSON());
  console.log("ent2", ent2.doc.toJSON());
};
log();

ent1.getOrInit(Name).set("first", "John");
ent1.commit();
ent2.getOrInit(Name).set("last", "Lawry");
ent1.commit();

setTimeout(() => {
  log();
  assertEquals(ent1.doc.toJSON(), ent2.doc.toJSON());

  ent1.getOrInit(Age).increment(6);
  ent1.commit();
  ent2.getOrInit(Age).decrement(4);
  ent2.commit();

  setTimeout(() => {
    log();
    assertEquals(ent1.doc.toJSON(), ent2.doc.toJSON());
  }, 0);
}, 0);
