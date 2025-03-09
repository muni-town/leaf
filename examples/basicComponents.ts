import { defComponent, LoroMap, LoroCounter, Entity } from "../src/leaf.ts";

// First we define our components

const Name = defComponent(
  // Components should all have globally unique identifiers.
  "01JNVY76XPH6Q5AVA385HP04G7",
  // Components are defined in terms of a Loro type
  LoroMap<{ first: string; last?: string }>,
  // We pass a function to initialize the component state to match it's type.
  (map) => map.set("first", "unnamed")
);

// Let's make age a counter
const Age = defComponent("01JNVYC0T0V6SDKDWQP51MYKS1", LoroCounter, (age) =>
  age.increment(1)
);

// Now we can create our entity, which is internally a Loro document.
const ent = new Entity();

// We can check which components an entity has
ent.has(Name); // false

// We can get a component, initializing it if it doesn't exist.
const name = ent.getOrInit(Name);

// We get then get fields on the component
name.get("first"); // unnamed
// And set them too!
name.set("last", "person");

// Happy birthday 🥳 🎂
ent.getOrInit(Age).increment(1);
