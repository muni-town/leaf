import decodeBase32 from "base32-decode";
import encodeBase32 from "base32-encode";

import {
  Container,
  LoroCounter,
  LoroDoc,
  LoroList,
  LoroMap,
  LoroMovableList,
  LoroText,
  LoroTree,
} from "loro-crdt";

export type EntityIdStr = `leaf:${string}`;

export type LoroContainerType =
  | LoroCounter
  | LoroList
  | LoroMap
  | LoroMovableList
  | LoroText
  | LoroTree;
type LoroContainerConstructor<T extends LoroContainerType> = new () => T;

type ComponentId = string;
type ComponentDef<T extends LoroContainerType> = {
  id: ComponentId;
  constructor: LoroContainerConstructor<T>;
  init: (container: T) => void;
};

function defComponent<T extends LoroContainerType>(
  id: string,
  constructor: LoroContainerConstructor<T>,
  init: (container: T) => void = () => {}
): ComponentDef<T> {
  return {
    id,
    constructor,
    init,
  };
}

export class EntityId {
  bytes: Uint8Array;

  /** Create a new Entity ID.
   *
   * If `id` is not specified a random ID will be generated.
   *
   * @argument id a string starting with `leaf:` and ending with 32 bytes encoded as a Crockford
   * base32 string.
   */
  constructor(id?: EntityIdStr) {
    if (id) {
      const data = new Uint8Array(decodeBase32(id, "Crockford"));
      if (data.length != 32)
        throw new Error(
          `Invalid byte length for Entity ID ( ${data.length} ), expected 32.`
        );
      this.bytes = data;
    } else {
      // For now we just pretend we're generating like a keypair and returning the public key, since
      // that's what Keyhive will need later.
      const bytes = new Uint8Array(32);
      crypto.getRandomValues(bytes);
      this.bytes = bytes;
    }
  }

  /** Get the entity ID as a Crockford base32 string. */
  toString(): EntityIdStr {
    return `leaf:${encodeBase32(this.bytes, "Crockford").toLowerCase()}`;
  }
}

type EntityDoc = LoroDoc<
  Record<string, Container> & {
    __components__: LoroMap<Record<string, true | undefined>>;
  }
>;
class Entity {
  id: EntityId;
  #doc: EntityDoc;

  get doc(): EntityDoc {
    return this.#doc;
  }

  constructor() {
    this.id = new EntityId();
    this.#doc = new LoroDoc();
  }

  has<T extends LoroContainerType>(def: ComponentDef<T>): boolean {
    return this.#doc.getMap("__components__").get(def.id) === true;
  }

  /** Delete a component from the entity */
  delete<T extends LoroContainerType>(def: ComponentDef<T>) {
    const { constructor, id } = def;
    this.#doc.getMap("__components__").delete(def.id);

    if (constructor === LoroCounter) {
      const counter = this.#doc.getCounter(id);
      counter.decrement(counter.value);
    } else if (constructor === LoroList) {
      this.#doc.getList(id).clear();
    } else if (constructor === LoroMap) {
      this.#doc.getMap(id).clear();
    } else if (constructor === LoroMovableList) {
      this.#doc.getMovableList(id).clear();
    } else if (constructor === LoroText) {
      const t = this.#doc.getText(id);
      t.splice(0, t.length, "");
    } else if (constructor === LoroTree) {
      const t = this.#doc.getTree(id);
      for (const root of t.roots()) {
        t.delete(root.id);
      }
    } else {
      throw new Error("Invalid constructor type when getting component");
    }
  }

  init<T extends LoroContainerType>(def: ComponentDef<T>) {
    const components = this.#doc.getMap("__components__");
    if (!components.get(def.id) == true) {
      const raw = this.#getRaw(def);
      def.init(raw);
      components.set(def.id, true);
    }
  }

  getOrInit<T extends LoroContainerType>(def: ComponentDef<T>): T {
    const raw = this.#getRaw(def);
    const components = this.#doc.getMap("__components__");
    if (components.get(def.id) !== true) {
      def.init(raw);
      components.set(def.id, true);
    }
    return raw;
  }

  get<T extends LoroContainerType>(def: ComponentDef<T>): T | undefined {
    if (!this.has(def)) return undefined;
    return this.#getRaw(def);
  }

  #getRaw<T extends LoroContainerType>({
    id,
    constructor,
  }: ComponentDef<T>): T {
    if (constructor === LoroCounter) {
      return this.#doc.getCounter(id) as T;
    } else if (constructor === LoroList) {
      return this.#doc.getList(id) as T;
    } else if (constructor === LoroMap) {
      return this.#doc.getMap(id) as T;
    } else if (constructor === LoroMovableList) {
      return this.#doc.getMovableList(id) as T;
    } else if (constructor === LoroText) {
      return this.#doc.getText(id) as T;
    } else if (constructor === LoroTree) {
      return this.#doc.getTree(id) as T;
    } else {
      throw new Error("Invalid constructor type when getting component");
    }
  }
}

const Name = defComponent("name", LoroMap<{ name: string }>, (map) =>
  map.set("name", "unnamed")
);
const Age = defComponent("age", LoroCounter, (age) => age.increment(1));
const Links = defComponent(
  "01JNVN9ZRHRCRXGXT33QA8Z9XK",
  LoroMap<{
    links: LoroMovableList<{ href: string; label?: string }>;
  }>,
  (comp) => {
    comp.setContainer("links", new LoroMovableList());
  }
);

const ent = new Entity();
console.log(ent.id.toString());
console.log(ent.has(Name));

const name = ent.getOrInit(Name);
name.set("name", "john");
console.log(name.toJSON());

const age = ent.getOrInit(Age);
console.log(age.value);
age.increment(1);
console.log(age.value);

console.log("name", ent.get(Name));

ent.delete(Name);

const links = ent.getOrInit(Links);

links.get("links").push({ href: "hello", label: "world" });

console.log("doc", ent.doc.toJSON());
