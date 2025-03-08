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
   * @argument id 32 bytes encoded as a Crockford base32 string.
   */
  constructor(id?: string) {
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
  toString(): string {
    return encodeBase32(this.bytes, "Crockford").toLowerCase();
  }
}

class Entity {
  id: EntityId;
  doc: LoroDoc<
    Record<string, Container> & {
      __components__: LoroMap<Record<string, true | undefined>>;
    }
  >;

  constructor() {
    this.id = new EntityId();
    this.doc = new LoroDoc();
  }

  has<T extends LoroContainerType>(def: ComponentDef<T>): boolean {
    return this.doc.getMap("__components__").get(def.id) === true;
  }

  delete<T extends LoroContainerType>(def: ComponentDef<T>) {
    this.doc.getMap("__components__").delete(def.id);
  }

  init<T extends LoroContainerType>(def: ComponentDef<T>) {
    const components = this.doc.getMap("__components__");
    if (!components.get(def.id) == true) {
      const raw = this.#getRaw(def);
      def.init(raw);
      components.set(def.id, true);
    }
  }

  getOrInit<T extends LoroContainerType>(def: ComponentDef<T>): T {
    const raw = this.#getRaw(def);
    const components = this.doc.getMap("__components__");
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
      return this.doc.getCounter(id) as T;
    } else if (constructor === LoroList) {
      return this.doc.getList(id) as T;
    } else if (constructor === LoroMap) {
      return this.doc.getMap(id) as T;
    } else if (constructor === LoroMovableList) {
      return this.doc.getMovableList(id) as T;
    } else if (constructor === LoroText) {
      return this.doc.getText(id) as T;
    } else if (constructor === LoroTree) {
      return this.doc.getTree(id) as T;
    } else {
      throw new Error("Invalid constructor type when getting component");
    }
  }
}

const Name = defComponent("name", LoroMap<{ value: string }>, (map) =>
  map.set("value", "unnamed")
);
const Age = defComponent("age", LoroCounter, (age) => age.increment(1));

const ent = new Entity();
console.log(ent.has(Name));

const name = ent.getOrInit(Name);
name.set("value", "john");
console.log(name.toJSON());

const age = ent.getOrInit(Age);
console.log(age.value);
age.increment(1);
console.log(age.value);

console.log("name", ent.get(Name));

ent.delete(Name);

console.log("name", ent.get(Name));
