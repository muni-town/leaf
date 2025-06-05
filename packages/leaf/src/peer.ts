import {
  Entity,
  EntityId,
  intoEntityId,
  IntoEntityId,
  EntityIdStr,
} from "./entity.ts";
import { StorageConfig, StorageManager } from "./storage.ts";
import { Syncer1 } from "./sync1.ts";

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
  #lastSavePromises = new Map<string, Promise<void>>();

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
      try {
        console.log("About to load from storage:", entIdStr, entity);
        const result = await storage.manager.load(entity);
        console.log("Loaded result:", result);
        loadedLocal = loadedLocal || result;
      } catch (err) {
        console.error("Storage.load threw:", err, "for entity:", entity);
        throw err; // or handle gracefully
      }
    }

    for (const syncer of this.#syncers) {
      // TODO: We need to make sure the syncer only holds a weak reference to the entity so that we
      // can detect when it's no longer needed and can be closed.
      syncer.sync(weakEntity);
    }

    const unsubscribeStorage = entity.doc.subscribe(() => {
      console.log("Peer.#open_raw subscription triggered");
      queueMicrotask(() => {
        const e = weakEntity.deref();
        if (!e) return;

        // Compose a single promise representing *all* saves for all storages
        const allSaves: Promise<void>[] = [];

        for (const storage of this.#storages) {
          if (storage.write === false) continue;

          const save = async () => {
            const ent = weakEntity.deref();
            if (ent) {
              await storage.manager.save(ent);
            }
          };

          if (storage.writeThrottle) {
            const p = new Promise<void>((resolve) => {
              storage.writeThrottle?.(async () => {
                await save();
                resolve();
              });
            });
            allSaves.push(p);
          } else {
            allSaves.push(save());
          }
        }

        // Track the collective save promise for this entity
        const savePromise = Promise.all(allSaves).then(() => {});
        this.#lastSavePromises.set(entIdStr, savePromise);
      });
    });

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

      if (this.#syncers.length > 0) {
        const syncPromises = this.#syncers
          .map((syncer) => syncer.syncing.get(entIdStr)?.awaitInitialLoad)
          .filter((p): p is Promise<void> => !!p);

        const race = [];

        if (syncPromises.length > 0) {
          race.push(Promise.race(syncPromises).then(stopTimeout));
        }

        if (timeoutPromise) {
          race.push(timeoutPromise);
        }

        // Only wait if there's something to wait on
        if (race.length > 0) {
          await Promise.race(race);
        }
      } else if (timeoutPromise) {
        // No syncers, but a timeout is specified—wait for it to expire before continuing
        await timeoutPromise;
      }
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
    this.#lastSavePromises.delete(entIdStr);
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
        console.log("Peer.close queueMicrotask", this);
        if (this) {
          const entId = id ? intoEntityId(id) : new EntityId();
          const entIdStr = entId.toString();

          const entity = (await this.#entities.get(entIdStr))?.deref();

          if (entity) {
            // This will trigger a write to storage
            entity.doc.commit();
            // Explicitly wait for the last save (if any)
            const lastSave = this.#lastSavePromises.get(entIdStr);
            if (lastSave) {
              await lastSave;
            }
            entity.free();
          }

          this.#rawUnload(entIdStr);
        }

        resolve();
      });
    });
  }
}
