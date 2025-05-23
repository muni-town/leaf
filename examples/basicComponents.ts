import assert from "node:assert";
import {
  defComponent,
  LoroMap,
  LoroCounter,
  Entity,
  Marker,
} from "@muni-town/leaf";

// First we define our components

const Name = defComponent(
  // Components should all have globally unique identifiers.
  // We add a human-readable prefix to make debugging easier.
  "name:01JNVY76XPH6Q5AVA385HP04G7",
  // Components are defined in terms of a Loro type
  LoroMap<{ first: string; last?: string }>,
  // We pass a function to initialize the component state to match its type.
  (map) => map.set("first", "unnamed")
);

// Let's make age a counter
const Age = defComponent("age:01JNVYC0T0V6SDKDWQP51MYKS1", LoroCounter, (age) =>
  age.increment(1)
);

// We can also make a marker component, where we only care about whether the component is on the
// entity, and we don't need to store any data in the component itself.
//
// This could be useful to track if an entity has been "soft deleted".
const SoftDelete = defComponent(
  "softDelete:01JPFQ2MXH4XJVTDT5ATEBQBQA",
  Marker
);

// Now we can create our entity, which is internally a Loro document.
const ent = new Entity();

// We can check which components an entity has
ent.has(Name); // false

// We can get a component, initializing it if it doesn't exist.
ent.getOrInit(Name, (name) => {
  // We get then get fields on the component
  name.get("first"); // unnamed
  // And set them too!
  name.set("last", "person");
});

// Happy birthday 🥳 🎂
ent.getOrInit(Age, (age) => age.increment(1));

// We don't have the marker component on the entity now
assert.equal(ent.has(SoftDelete), false);

// But we can add it by using the `init` function.
ent.init(SoftDelete);
assert.equal(ent.has(SoftDelete), true);

// And we can delete it to remove it.
ent.delete(SoftDelete);
assert.equal(ent.has(SoftDelete), false);
