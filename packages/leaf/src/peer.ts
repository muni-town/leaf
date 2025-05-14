import {
  Entity,
  EntityId,
  EntityIdStr,
  IntoEntityId,
  intoEntityId,
} from "./entity.ts";
import { StorageManager, StorageConfig } from "./storage.ts";
import { Syncer1 } from "./sync1.ts";

export type PeerOption = StorageManager | Syncer1 | StorageConfig;

/**
 * Options for configuring {@linkcode Peer.open}.
 *
 * @see defaultPeerOpenOptions;
 */
export type PeerOpenOptions = {
  /** Whether when opening the entity we should wait to return until we have gotten an update from
   * one of our syncers, or else the `awaitSyncTimeout` passes. */
  awaitSync: boolean;
  /** The amount of time we should wait for a peer to sync us an update before just continuing when
   * `awaitSync` is `true`. */
  awaitSyncTimeout: number;
};
/** Default {@linkcode PeerOpenOptions}. */
export const defaultPeerOpenOptions = {
  awaitSync: true,
  awaitSyncTimeout: 1000,
};

/**
 * The entrypoint for opening {@linkcode Entity}s, loading and saving them automatically from
 * {@linkcode StorageManager}s, and syncing them with {@linkcode Syncer1}s.
 */
export class Peer {
  #syncers: Syncer1[] = [];
  #storages: StorageConfig[] = [];
  #storageUnsubscribers: Map<EntityIdStr, () => void> = new Map();
  #entities: Map<EntityIdStr, WeakRef<Entity>> = new Map();
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
   * It will also start syncing the entity with all of the peer's syncers.
   * */
  async open(
    id?: IntoEntityId,
    opts?: Partial<PeerOpenOptions>
  ): Promise<Entity> {
    const options = { ...defaultPeerOpenOptions, ...(opts || {}) };

    const entId = id ? intoEntityId(id) : new EntityId();
    const entIdStr = entId.toString();

    const existingEnt = this.#entities.get(entIdStr)?.deref();
    if (existingEnt) return existingEnt;

    const entity = new Entity(entId);
    const weakEntity = new WeakRef(entity);
    this.#finalizationRegistry.register(entity, entIdStr, weakEntity);
    this.#entities.set(entIdStr, weakEntity);
    for (const storage of this.#storages) {
      if (storage.read !== false) {
        await storage.manager.load(entity);
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

    if (options.awaitSync) {
      let stopTimeout: () => void = () => {};

      // Finish with the first to complete
      await Promise.race([
        // Wait until all syncers have responded with their latest data for the entity
        (async () => {
          await Promise.all(
            this.#syncers.map(
              (syncer) => syncer.syncing.get(entIdStr)?.awaitInitialLoad
            )
          );
          stopTimeout();
        })(),

        // Just timeout if we don't get a response in time
        new Promise((r) => {
          const n = setTimeout(r, options.awaitSyncTimeout);
          stopTimeout = () => {
            r(undefined);
            clearTimeout(n);
          };
        }),
      ]);
    }

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
      queueMicrotask(() => {
        if (this) {
          const entId = id ? intoEntityId(id) : new EntityId();
          const entIdStr = entId.toString();

          const entity = this.#entities.get(entIdStr)?.deref();

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
