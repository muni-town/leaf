import { defComponent, LoroCounter, LoroMap } from "../src/index.ts";

export const Name = defComponent(
  "name:01JNVY76XPH6Q5AVA385HP04G7",
  LoroMap<{ first: string; last?: string }>,
  (map) => map.set("first", "unnamed")
);

export const Age = defComponent("age:01JNYE9SF7Z9VDSH8N1PRYEEMP", LoroCounter);
