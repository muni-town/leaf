/**
 * Synchronization tools that can be used to sync documents over the network with other peers.
 *
 * **Note:** This module is called `sync1` because it is a temporary placeholder for the more
 * permanent sync implementation that will come later and be built, most-likely on
 * [Keyhive](https://www.inkandswitch.com/beehive/notebook/).
 *
 * The name is annoying, but is intentionally temporary.
 *
 * @module
 */

import { Entity, EntityIdStr } from "./leaf.ts";
import { getOrDefault } from "./utils.ts";

/**
 * An implementation of an entity synchronizer. A synchronizer allows you to sync entities with other
 * peers, possibly over the network, etc.
 *
 * Different syncers can be used to sync an entity across different protocols or connection types.
 */
export interface Sync1Interface {
  /**
   * Get the latest snapshots of the given entity and provide our latest snapshot as well.
   *
   * This will be called once when an entity is first starting to sync to get the latest known
   * state, and to update the remote with our latest state.
   *
   * **TODO:** This needs to be made more sophisticated so that the entire snapshot doesn't need to
   * be shared over the network when partial updates can be synced. At the same time, we may just
   * want to wait for Sedimentree.
   *
   * @param entityId The entity ID to fetch snapshots for.
   */
  syncSnapshots(
    entityId: EntityIdStr,
    snapshot: Uint8Array
  ): Promise<Uint8Array[]>;

  /**
   * Subscribe to updates for a given entity.
   *
   * Every time the sync interface has an update for the entity it will call `handleUpdate`, passing
   * it the entity ID and the update.
   *
   * @param entityId The entity ID to subscribe to.
   * @param handleUpdate The handler function to be invoked when the sync interface has a new
   * update.
   * @returns Returns a function that may be called to unsubscribe.
   * */
  subscribe(
    entityId: EntityIdStr,
    handleUpdate: (entityId: EntityIdStr, update: Uint8Array) => void
  ): () => void;

  /**
   * Send an update for a given entity. This must be called to send local changes to remote peers.
   * */
  sendUpdate(entityId: EntityIdStr, update: Uint8Array): void;
}

/**
 * A syncer that can be used to keep {@linkcode Entity}s up-to-date with other peers across a sync
 * interface.
 */
export class Syncer1 {
  inter: Sync1Interface;

  syncing: Map<
    EntityIdStr,
    {
      entity: Entity;
      unsubscribe: () => void;
    }
  >;

  /** Create a new syncer using the provided {@linkcode Sync1Interface}. */
  constructor(sync: Sync1Interface) {
    this.inter = sync;
    this.syncing = new Map();
  }

  /**
   * Start syncing an entity. All local updates will be pushed to peers, and incoming changes will
   * be automatically merged into the entity.
   * */
  sync(entity: Entity) {
    const id = entity.id.toString();

    if (this.syncing.has(id)) return;

    let earlyUnsubscribe = false;
    this.syncing.set(id, {
      entity,
      unsubscribe: () => {
        earlyUnsubscribe = true;
      },
    });

    // Subscribe to Loro changes and send them to peers
    const unsubscribeLoro = entity.doc.subscribeLocalUpdates((update) => {
      // NOTE: This queueMicrotask turns out important, interestingly. The `subscribeLocalUpdates`
      // callback is triggered by the Rust WASM module to trigger JS code, and it suspends the Rust
      // code, waiting for this JS function to return, before resuming it's callstack ( or something
      // like that ) in the Rust module.
      //
      // If we don't queue the handling of the update, then calling `this.inter.sendUpdate` may try
      // to trigger code from the Rust WASM module again, while it is still technically in this
      // function callback and in a suspended callstack waiting for this JS callback to finish.
      //
      // Since it's still "suspended" and yet we're trying to run another function before it
      // finishes, WASM bindgen will throw an error because that could cause aliasing issues.
      //
      // Queuing the microtask will allow the callback to finish immediately so that the WASM module
      // is ready to accept other calls by the time the microtask is run.
      queueMicrotask(() => {
        this.inter.sendUpdate(id, update);
      });
    });

    (async () => {
      const syncing = this.syncing.get(id);
      if (!syncing) return unsubscribeLoro();
      if (earlyUnsubscribe) return unsubscribeLoro();

      // Subscribe to updates from the network
      const unsubscribeNet = this.inter.subscribe(id, (_id, update) => {
        entity.doc.import(update);
      });
      // Record unsubscribe functions for later
      syncing.unsubscribe = () => {
        unsubscribeNet();
        unsubscribeLoro();
      };

      // Fetch initial snapshots and apply them.
      const snapshots = await this.inter.syncSnapshots(
        id,
        entity.doc.export({ mode: "snapshot" })
      );
      entity.doc.importBatch(snapshots);
    })();
  }

  /** Stop syncing an entity. */
  unsync(id: EntityIdStr) {
    const syncing = this.syncing.get(id);
    if (!syncing) return;
    syncing.unsubscribe();
    this.syncing.delete(id);
  }
}

type MemorySync1Adaptr = Sync1Interface & {
  entities: Map<EntityIdStr, Entity>;
  subscribers: Map<
    EntityIdStr,
    ((id: EntityIdStr, update: Uint8Array) => void)[]
  >;
};

export const memorySync1Adapters = (count = 2): MemorySync1Adaptr[] => {
  const interfaces: MemorySync1Adaptr[] = [];

  for (let i = 0; i < count; i++) {
    interfaces[i] = {
      entities: new Map(),
      subscribers: new Map(),
      syncSnapshots(id, snapshot) {
        let entity = this.entities.get(id);
        if (!entity) {
          entity = new Entity(id);
        }
        entity.doc.import(snapshot);

        return Promise.resolve([entity.doc.export({ mode: "snapshot" })]);
      },
      sendUpdate(id, update) {
        const entity = this.entities.get(id);
        if (entity) entity.doc.import(update);
        for (let j = 0; j < count; j++) {
          if (j !== i) {
            const subs = getOrDefault(interfaces[j].subscribers, id, []);
            for (const notify of subs) {
              notify(id, update);
            }
          }
        }
      },
      subscribe(id, handleUpdate) {
        getOrDefault(this.subscribers, id, []).push(handleUpdate);
        return () => {
          this.subscribers.set(
            id,
            getOrDefault(this.subscribers, id, []).filter(
              (x) => x !== handleUpdate
            )
          );
        };
      },
    };
  }

  return interfaces;
};
