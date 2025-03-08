import decodeBase32 from "base32-decode";
import encodeBase32 from "base32-encode";

import { LoroDoc } from "loro-crdt";

type ComponentId = string;
type Component<T> = { id: ComponentId; data: T };
type ComponentDef<T> = ((data: T) => Component<T>) & {
  id: ComponentId;
  delete: () => ComponentDelete;
};
type ComponentDelete = { delete: ComponentId };
type ComponentUpdate<T> = Component<T> | ComponentDelete;

function defComponent<T>(id: string): ComponentDef<T> {
  const constructor = (data: T) => ({ id, data });
  constructor.id = id;
  constructor.delete = () => ({ delete: id });
  return constructor;
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
  components: { [id: ComponentId]: unknown };

  constructor(...components: Component<unknown>[]) {
    this.id = new EntityId();
    this.components = {};
    for (const { id, data } of components) {
      this.components[id] = data;
    }
  }

  update(...updates: ComponentUpdate<unknown>[]) {
    for (const update of updates) {
      if ("delete" in update) {
        delete this.components[update.delete];
      } else {
        this.components[update.id] = update.data;
      }
    }
  }

  get<T>(def: ComponentDef<T>): T | undefined {
    return this.components[def.id] as T | undefined;
  }
}

const Name = defComponent<string>("name");
const LastName = defComponent<string>("LastName");
const Age = defComponent<number>("age");

const ent = new Entity(Name("john"), Age(5));

console.log(ent)
