import { Entity, memorySync1Adapters, Syncer1 } from "@muni-town/leaf";
import { Age, Name } from "./components";
import assert from "node:assert"

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

ent1.getOrInit(Name, (name) => name.set("first", "John"));
ent1.commit();
ent2.getOrInit(Name, (name) => name.set("last", "Lawry"));
ent1.commit();

setTimeout(() => {
  log();
  assert.deepEqual(ent1.doc.toJSON(), ent2.doc.toJSON());

  ent1.getOrInit(Age, (age) => age.increment(6));
  ent1.commit();
  ent2.getOrInit(Age, (age) => age.decrement(4));
  ent2.commit();

  setTimeout(() => {
    log();
    assert.deepEqual(ent1.doc.toJSON(), ent2.doc.toJSON());
  }, 0);
}, 0);
