/**
 * Welcome to the Leaf SDK! Leaf is an opinionated wrapper around CRDT tools for building
 * local-first apps!
 *
 * Leaf is built around an Entity-Component data model designed to help you build inter-operable
 * apps.
 *
 * If you are new to Leaf, first check out {@linkcode defComponent} and then look at
 * {@linkcode Entity} to get an idea of how things are put together. Finally you'll want to use a
 * {@linkcode Peer} in most cases to sync your entities with storage and sync interfaces.
 *
 * Leaf is currently built on [Loro](https://loro.dev) as its underlying CRDT, and
 * {@link Entity.doc} is in fact a {@linkcode LoroDoc}, so reading the Loro documentation will be
 * necessary to understand how to fully interact with the Leaf entity data.
 *
 * @module @muni-town/leaf
 */

import decodeBase32 from "base32-decode";
import encodeBase32 from "base32-encode";

export * from "loro-crdt";
export * from "./sync1.ts";
export * from "./storage.ts";
export * from "./sync-proto.ts";
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
import { Syncer1 } from "./sync1.ts";
import { StorageManager } from "./storage.ts";

/** The prefix for the string representation of {@linkcode EntityId}s. */
export const entityIdPrefix = "leaf:" as const;

/** String representation of an {@linkcode EntityId}. */
export type EntityIdStr = `${typeof entityIdPrefix}${string}`;

/** A Loro CRDT type that may be used as the type of a {@link defComponent|component}. */
export type ComponentType =
  | LoroCounter
  | LoroList
  | LoroMap
  | LoroMovableList
  | LoroText
  | LoroTree
  | Marker;
/** A constructor for a {@linkcode ComponentType}. */
export type ComponentConstructor<T extends ComponentType> = new () => T;

/**
 * A {@linkcode ComponentType} that doesn't have any data.
 *
 * This is useful for making "marker" components where you only care whether or not the component is
 * added to an entity and there is no other data to store in the component itself.
 * */
export class Marker {
  constructor() {}
}

/** The ID for a component. */
export type ComponentId = string;

/**
 * A component definition.
 *
 * Component definitions have the information needed to get and initialize the component on an Entity.
 *
 * Use {@linkcode defComponent} to create a new {@linkcode ComponentDef}.
 */
export type ComponentDef<T extends ComponentType> = {
  id: ComponentId;
  constructor: ComponentConstructor<T>;
  init: (container: T) => void;
};

/**
 * Define a new component type.
 *
 * All data in Leaf is made up of components. These components are meant to be small, re-usable, and
 * semantically meaningful pieces of data, such as a `Name`, `Image`, or `Description`.
 *
 * Before you can use a component you must define it, which sets its unique ID, its type, and its
 * initialization function.
 *
 * ## Example
 *
 * Here's an example of a `Name` component that requires a first name and optionally has a last
 * name.
 *
 * ```ts
 * export const Name = defComponent(
 *    "name:01JNVY76XPH6Q5AVA385HP04G7",
 *    LoroMap<{ first: string; last?: string }>,
 *    (map) => map.set("first", "")
 * );
 * ```
 *
 * After defining a component you may use it with an {@linkcode Entity}.
 *
 * Let's break it down piece-by-piece.
 *
 * #### Exported Variable
 * Notice that we store the definition in a variable and we export it from the module. This allows
 *   us to use the same component definition throughout the codebase, and even in other projects
 *   that might depend on this module, if we are writing a library.
 *
 * This is not required, of course, but will often be useful in real projects.
 *
 * #### Unique ID
 * The unique ID makes sure that this component is distinguished from any other component that might
 *   exist. By using exported variables and unique IDs, we don't have to worry about conflicting
 *   names for different components.
 *
 * #### Component Type
 * The next argument is the type we want to use for the component. This must be one of the
 *   {@linkcode ComponentType}s and should usually include extra type annotations describing the
 *   data that will go inside.
 *
 * In this example we say that `Name` is a {@linkcode LoroMap} and we annotate the inner data as
 * requiring a `string` `first` name and optionally having a `string` `last` name.
 *
 * Note that components must be a so called "container type", such as a map, array, rich text, etc.
 * If you wish to store only one primitive value such as a `string` or `number` in a component, you
 * can always store it in a map with one field.
 *
 * #### Initialization Function
 *
 * Finally, the last argument is an initialization function. When the component gets created it will
 * be the empty, default value of whatever {@linkcode ComponentType} that you specified.
 *
 * In this case, that means `Name` will start off as an empty map which, notably, does not match our
 * type annotation of requiring a first name.
 *
 * Therefore it is important, when annotating your component type, that you also supply an
 * initialization function to set any required fields so that your returned component will match
 * your annotated type.
 *
 * In this case, we just initialize the first name to an empty string.
 *
 * @param id The globally unique ID of the component type. Often this will include a
 * [ULID](https://ulidgenerator.com/) or [UUID](https://www.uuidgenerator.net/) prefixed by a short
 * human-readable name to assist in debugging.
 * @param constructor One of the {@linkcode ComponentType}s. This will be used to construct the
 * component initially, in addition to the `init` function if provided.
 * @param init An optional function that will be called to initialize the component when it is first
 * added to an Entity. Since the constructor will initialize an empty version of whatever type you
 * select, you must use this init function if you want to make sure that it has any of the initial
 * data necessary to match your annotated type.
 * @returns A component definition that can be used to add, edit, and delete components on an
 * {@linkcode Entity}.
 */
export function defComponent<T extends ComponentType>(
  id: string,
  constructor: ComponentConstructor<T>,
  init: (container: T) => void = () => {}
): ComponentDef<T> {
  return {
    id,
    constructor,
    init,
  };
}

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

/** A policy describing how to  */
export type StorageConfig = {
  /** Whether this storage should be read from when loading documents. Defaults to `true`. */
  read?: boolean;
  /** Whether this storage should be written to when documents change. Defaults to `true`. */
  write?: boolean;
  /**
   * Custom throttle function that can be used to debounce or throttle writes to this storage. The
   * function should call `write()` when it wants to actually trigger the pending write to storage.
   */
  writeThrottle?: (write: () => void) => void;
  /** The storage manager to use. */
  manager: StorageManager;
};

export type PeerOption = StorageManager | Syncer1 | StorageConfig;

/**
 * Options for configuring {@linkcode Peer["open"]}.
 */
export type PeerOpenOptions = {
  /**
   * If set, when opening an entity that does not exist locally, and cannot be synced from a peer,
   * the function will wait the specified number of milliseconds before creating the entity and
   * returning the newly created entity.
   * */
  createAfterTimeout: number;
};

/**
 * The entrypoint for opening {@linkcode Entity}s, loading and saving them automatically from
 * {@linkcode StorageManager}s, and syncing them with {@linkcode Syncer1}s.
 */
export class Peer {
  #syncers: Syncer1[] = [];
  #storages: StorageConfig[] = [];
  #storageUnsubscribers: Map<EntityIdStr, () => void> = new Map();
  #entities: Map<EntityIdStr, Promise<WeakRef<Entity>>> = new Map();
  #finalizationRegistry: FinalizationRegistry<EntityIdStr>;

  constructor(...options: PeerOption[]) {
    this.#finalizationRegistry = new FinalizationRegistry(this.close);
    for (const option of options) {
      if (option instanceof StorageManager) {
        this.#storages.push({
          manager: option,
        });
      } else if (option instanceof Syncer1) {
        this.#syncers.push(option);
      } else if ("manager" in option) {
        this.#storages.push(option);
      }
    }
  }

  /**
   * Open the entity with the given ID.
   *
   * This will try to load the entity from all read storages and await on that before returning.
   *
   * If the entity is not found the promise will remain pending.
   *
   * It will also start syncing the entity with all of the peer's syncers.
   * */
  async open(
    id: IntoEntityId,
    options?: Partial<PeerOpenOptions>
  ): Promise<Entity> {
    // console.debug("calling open", intoEntityId(id).toString(), options);
    return this.#open_raw(id, options);
  }

  async create(): Promise<Entity> {
    // console.debug("calling create");
    const entity = this.#open_raw();
    // console.debug("created", (await entity).id.toString());
    return entity;
  }

  async #open_raw(
    id?: IntoEntityId,
    opts?: Partial<PeerOpenOptions>
  ): Promise<Entity> {
    const options = { ...(opts || {}) };

    const entId = id ? intoEntityId(id) : new EntityId();
    // console.debug("opening raw", entId.toString(), options);
    const entIdStr = entId.toString();

    const existingEntPromise = this.#entities.get(entIdStr);
    if (existingEntPromise) {
      const weakExistingEnt = await existingEntPromise;
      const existingEnt = weakExistingEnt.deref();
      if (existingEnt) {
        // console.debug("Already open", entId.toString());
        return existingEnt;
      } else {
        this.#entities.delete(entIdStr);
      }
    }

    const entity = new Entity(entId);
    const weakEntity = new WeakRef(entity);
    this.#finalizationRegistry.register(entity, entIdStr, weakEntity);
    let resolveWeakEntity: (e: WeakRef<Entity>) => void = () => {};
    this.#entities.set(entIdStr, new Promise((r) => (resolveWeakEntity = r)));

    let loadedLocal = id ? false : true; // We've already "loaded" the entity locally if we just created it
    for (const storage of this.#storages) {
      if (storage.read !== false) {
        loadedLocal = loadedLocal || (await storage.manager.load(entity));
      }
    }

    for (const syncer of this.#syncers) {
      // TODO: We need to make sure the syncer only holds a weak reference to the entity so that we
      // can detect when it's no longer needed and can be closed.
      syncer.sync(weakEntity);
    }

    // Sync to storages on doc change.
    const unsubscribeStorage = entity.doc.subscribe(() => {
      queueMicrotask(async () => {
        for (const storage of this.#storages) {
          if (storage.write !== false) {
            const save = async () => {
              const e = weakEntity.deref();
              if (e) await storage.manager.save(e);
            };

            if (storage.writeThrottle) {
              storage.writeThrottle(save);
            } else {
              await save();
            }
          }
        }
      });
    });
    this.#storageUnsubscribers.set(entIdStr, unsubscribeStorage);

    if (!loadedLocal) {
      let stopTimeout: () => void = () => {};

      // Just timeout if we don't get a response in time
      const timeoutPromise =
        options.createAfterTimeout &&
        new Promise<void>((r) => {
          const n = setTimeout(r, options.createAfterTimeout);
          stopTimeout = () => {
            r(undefined);
            clearTimeout(n);
          };
        });

      const race = [
        // Wait until a syncer has responded with their latest data for the entity
        (async () => {
          const promises = this.#syncers.map((syncer) =>
            (async () => {
              // console.log("staring sync", entId.toString());
              await syncer.syncing.get(entIdStr)?.awaitInitialLoad;
              // console.log("done syncing", entId);
              return;
            })()
          );
          await Promise.race(promises);
          stopTimeout();
        })(),
      ];
      if (timeoutPromise) race.push(timeoutPromise);

      // Finish with the first to complete
      await Promise.race(race);
    } else {
      // console.debug("loaded local", entId.toString());
    }

    resolveWeakEntity(weakEntity);
    // console.debug(
    //   "returning entity ",
    //   entId.toString(),
    //   entity.doc.changeCount()
    // );
    return entity;
  }

  #rawUnload(entIdStr: EntityIdStr) {
    const unsubscribeStorage = this.#storageUnsubscribers.get(entIdStr);
    if (unsubscribeStorage) unsubscribeStorage();
    for (const syncer of this.#syncers) {
      syncer.unsync(entIdStr);
    }
    const entity = this.#entities.get(entIdStr);
    if (entity) this.#finalizationRegistry.unregister(entity);
    this.#entities.delete(entIdStr);
  }

  /**
   * Delete an entity completely from local storage.
   *
   * > **TODO:** Currently there is no way to have sync peers receive a request to delete an entity.
   * > It is only deleted locally.
   * */
  async delete(id: IntoEntityId) {
    const entId = intoEntityId(id);
    const entIdStr = entId.toString();
    this.#rawUnload(entIdStr);

    for (const storage of this.#storages) {
      if (storage.write !== false) {
        await storage.manager.delete(id);
      }
    }
  }

  /** Commit the entity, stop syncing it, and flush it to storage. */
  close(id: IntoEntityId): Promise<void> {
    return new Promise((resolve) => {
      // NOTE: we queue a microtask here because if you have _just_ committed an entity, and then
      // you call this function, the change callbacks on the entity have not yet bent run, and the
      // network / storage synchronization has not been finished yet. So we queue the actual cleanup
      // of the entity until _after_ the other callbacks have run, with queueMicrotask, so that the
      // caller can wait for the entity to actually finish getting persisted.
      queueMicrotask(async () => {
        if (this) {
          const entId = id ? intoEntityId(id) : new EntityId();
          const entIdStr = entId.toString();

          const entity = (await this.#entities.get(entIdStr))?.deref();

          if (entity) {
            // This will trigger a write to storage
            entity.doc.commit();
            // For the same reason mentioned in the nte above, we have to wait until the next tick
            // to actually free the entity after the commit.
            queueMicrotask(() => {
              entity.free();
            });
          }

          this.#rawUnload(entIdStr);
        }

        resolve();
      });
    });
  }
}
