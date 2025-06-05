import decodeBase32 from "base32-decode";
import encodeBase32 from "base32-encode";

import {
  type Container,
  LoroCounter,
  LoroDoc,
  LoroList,
  LoroMap,
  LoroMovableList,
  LoroText,
  LoroTree,
} from "loro-crdt";

import { ComponentType, ComponentDef, Marker } from "./component.ts";

/** The prefix for the string representation of {@linkcode EntityId}s. */
export const entityIdPrefix = "leaf:" as const;

/** String representation of an {@linkcode EntityId}. */
export type EntityIdStr = `${typeof entityIdPrefix}${string}`;

/** A type that can be converted to an {@linkcode EntityId} */
export type IntoEntityId = Entity | EntityIdStr | EntityId;
/** A helper to convert a compatible type to an {@linkcode EntityId}. */
export function intoEntityId(id: IntoEntityId): EntityId {
  return id instanceof EntityId
    ? id
    : id instanceof Entity
    ? id.id
    : new EntityId(id);
}

/**
 * The ID of an {@linkcode Entity}.
 *
 * In string from an Entity ID looks like this:
 *
 *     leaf:ey02v80j9x376qgcczy8sq0pwvdbx01kbx0n7nbj90f87fnj5c50
 *
 * Leaf entity IDs always start with `leaf:` and end with a Crockford base32 encoded sequence of 32
 * bytes.
 *
 * Currently these are random bytes, in the future they will be public keys.
 * */
export class EntityId {
  /** The raw bytes of the Entity ID. */
  bytes: Uint8Array;

  /** Create a new Entity ID.
   *
   * If `id` is not specified a random ID will be generated.
   *
   * @param id a string starting with `leaf:` and ending with 32 bytes encoded as a Crockford
   * base32 string.
   */
  constructor(id?: EntityIdStr) {
    if (id) {
      if (!id.startsWith(entityIdPrefix))
        throw new Error(`Entity ID must start with \`${entityIdPrefix}\``);

      const data = new Uint8Array(
        decodeBase32(id.slice(entityIdPrefix.length), "Crockford")
      );
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

  /** Get the string formatted Entity ID */
  toString(): EntityIdStr {
    return `leaf:${encodeBase32(this.bytes, "Crockford").toLowerCase()}`;
  }
}

/** The key under which the list of components in an {@linkcode Entity} are stored in its internal
 * {@linkcode LoroDoc}.
 *
 * @internal
 * */
export const entityComponentsKey = "___leaf_components___";

/** The type of the Loro doc for {@linkcode Entity}s. */
export type EntityDoc = LoroDoc<
  Record<string, Container> & {
    [entityComponentsKey]: LoroMap<Record<string, true | undefined>>;
  }
>;

const entityRegistry = new FinalizationRegistry<EntityDoc>((doc) => doc.free());
/**
 * An entity.
 *
 * Entities are containers for collections of {@link defComponent|Components}. All data in Leaf is
 * stored in components that have been attached to {@linkcode Entity}s.
 *
 * ## Example
 *
 *  For example, assuming you have defined a `Name` component as demonstrated in the
 *  {@linkcode defComponent} example, you could add it to a new entity like so:
 *
 * ```ts
 * const ent = new Entity();
 *
 * const name = ent.getOrInit(Name)
 * name.set("first", "John")
 * name.set("last", "Smith")
 * ```
 */
export class Entity {
  /** The unique {@linkcode EntityId} for this entity. */
  #id: EntityId;
  #doc: EntityDoc;

  /**
   * Get the internal {@linkcode LoroDoc}.
   *
   * This can be used to do anything that Loro allows, such as exporting snapshots, doing time
   * travel, etc.
   *
   * ### Entity Storage Format
   * **Note:** The {@linkcode Entity}'s components are stored in this doc. If you modify the
   * components directly through the {@linkcode LoroDoc} you may confuse the normal entity API when
   * it tries to read or write components.
   *
   * In the Loro doc the `Entity` stores one special container using the value in
   * {@linkcode entityComponentsKey} as its ID. It is important that no component use this key as
   * its ID.
   *
   * This container will be a map that contains an entry for each component ID on the entity. The
   * value will always be `true`. For every key in that map, the component is considered to be on
   * the entity.
   *
   * Every other root container in the Loro document will be a component, and its ID will be the
   * component ID.
   */
  get doc(): EntityDoc {
    return this.#doc;
  }

  /** The Entity's unique ID. */
  get id(): EntityId {
    return this.#id;
  }

  /**
   * Create a new {@linkcode Entity}.
   *
   * By default the entity will have a new random ID and be initialize with no components.
   *
   * @param id Specifying the ID will create an entity with a specific ID instead of a random ID.
   * The document will sill be empty, by default, so if you are loading a specific entity you may
   * need to load the entity with a {@linkcode StorageManager}, for instance.
   */
  constructor(id?: IntoEntityId) {
    this.#id = id ? intoEntityId(id) : new EntityId();
    this.#doc = new LoroDoc();
    entityRegistry.register(this, this.#doc, this);
  }

  /**
   * Manually garbage collect this entity. Shouldn't usually be necessary to call this but can be
   * used to free memory more quickly instead of waiting for the periodic collector to come clean it
   * up. */
  free() {
    entityRegistry.unregister(this);
    this.#doc.free();
  }

  /**
   * Check whether the entity has a given component on it.
   *
   * ```ts
   * if (!entity.has(Name)) throw "Person must have name!";
   * ```
   * */
  has<T extends ComponentType>(def: ComponentDef<T>): boolean {
    const components = this.#doc.getMap(entityComponentsKey);
    const result = components.get(def.id) === true;
    components.free();
    return result;
  }

  /** Delete a component from the entity. */
  delete<T extends ComponentType>(def: ComponentDef<T>) {
    const { constructor, id } = def;
    const components = this.#doc.getMap(entityComponentsKey);
    components.delete(def.id);
    components.free();

    if (constructor === LoroCounter) {
      const counter = this.#doc.getCounter(id);
      counter.decrement(counter.value);
      counter.free();
    } else if (constructor === LoroList) {
      const list = this.#doc.getList(id);
      list.clear();
      list.free();
    } else if (constructor === LoroMap) {
      const map = this.#doc.getMap(id);
      map.clear();
      map.free();
    } else if (constructor === LoroMovableList) {
      const list = this.#doc.getMovableList(id);
      list.clear();
      list.free();
    } else if (constructor === LoroText) {
      const t = this.#doc.getText(id);
      t.splice(0, t.length, "");
      t.free();
    } else if (constructor === LoroTree) {
      const t = this.#doc.getTree(id);
      for (const root of t.roots()) {
        t.delete(root.id);
      }
      t.free();
    } else if (constructor === Marker) {
      // We don't need to do anything since we already removed the component from the components
      // list.
    } else {
      throw new Error("Invalid constructor type when getting component");
    }
  }

  /**
   * Initialize a component with its default value and add it to the entity, if the entity does not
   * already have a component of that type.
   * */
  init<T extends ComponentType>(def: ComponentDef<T>): Entity {
    const components = this.#doc.getMap(entityComponentsKey);
    if (!components.get(def.id) == true) {
      const raw = this.#getRaw(def);
      def.init(raw);
      components.set(def.id, true);
    }
    components.free();
    return this;
  }

  /**
   * Get the component of the given type on the entity, initializing it with its default value if it
   * does not already exist on the entity.
   *
   * The `handler` function will be immediately called with the retrieved component and the return
   * value of that handler will be returned from by this function.
   *
   * This allows us to automatically reclaim the component accessor memory without waiting for the
   * garbage collector.
   * */
  getOrInit<T extends ComponentType, R>(
    def: ComponentDef<T>,
    handler: (component: T) => R
  ): R {
    const raw = this.#getRaw(def);
    const components = this.#doc.getMap(entityComponentsKey);
    if (components.get(def.id) !== true) {
      def.init(raw);
      components.set(def.id, true);
    }
    components.free();
    const ret = handler(raw);
    if (!(raw instanceof Marker) && raw !== (ret as any)) raw.free();
    return ret;
  }

  /** Get the component of the given type from the entity, or `undefined` if the component is not on
   * the entity. */
  get<T extends ComponentType, R>(
    def: ComponentDef<T>,
    handler: (component: T) => R
  ): R | undefined {
    if (!this.has(def)) return undefined;
    const raw = this.#getRaw(def);
    const ret = handler(raw);
    if (!(raw instanceof Marker) && raw != (ret as any)) raw.free();
    return ret;
  }

  commit(options?: Parameters<typeof this.doc.commit>[0]) {
    this.doc.commit(options);
  }

  /**
   * Register a callback that will be run when the entity is committed.
   * @returns A function that may be called to unregister the callback.
   */
  subscribe(listener: () => void): () => void {
    return this.doc.subscribe(() => listener());
  }

  #getRaw<T extends ComponentType>({ id, constructor }: ComponentDef<T>): T {
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
    } else if (constructor === Marker) {
      return new Marker() as T;
    } else {
      throw new Error("Invalid constructor type when getting component");
    }
  }
}
