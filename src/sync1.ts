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

import { Entity, EntityIdStr, LoroDoc, StorageConfig } from "./leaf.ts";
import { StorageManager } from "./storage.ts";
import { getOrDefault } from "./utils.ts";

export * as proto from "./sync1/proto.ts";

export type Subscriber = (entityId: EntityIdStr, update: Uint8Array) => void;

/**
 * An implementation of an entity synchronizer. A synchronizer allows you to sync entities with other
 * peers, possibly over the network, etc.
 *
 * Different syncers can be used to sync an entity across different protocols or connection types.
 */
export interface Sync1Interface {
  /**
   * Subscribe to updates for a given entity.
   *
   * Every time the sync interface has an update for the entity it will call `handleUpdate`, passing
   * it the entity ID and the update.
   *
   * @param entityId The entity ID to subscribe to.
   * @param localSnapshot The latest snapshot that the subscriber has locally.
   * @param handleUpdate The handler function to be invoked when the sync interface has a new
   * update.
   * @returns Returns a function that may be called to unsubscribe.
   * */
  subscribe(
    entityId: EntityIdStr,
    localSnapshot: Uint8Array,
    handleUpdate: Subscriber
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
  sync(entity: Entity): void {
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

    const syncing = this.syncing.get(id);
    if (!syncing) return unsubscribeLoro();
    if (earlyUnsubscribe) return unsubscribeLoro();

    // Subscribe to updates from the network
    const unsubscribeNet = this.inter.subscribe(
      id,
      entity.doc.export({ mode: "snapshot" }),
      (_id, update) => {
        entity.doc.import(update);
      }
    );
    // Record unsubscribe functions for later
    syncing.unsubscribe = () => {
      unsubscribeNet();
      unsubscribeLoro();
    };

    // Fetch initial snapshots and apply them.
    this.inter.sendUpdate(id, entity.doc.export({ mode: "snapshot" }));
  }

  /** Stop syncing an entity. */
  unsync(id: EntityIdStr) {
    const syncing = this.syncing.get(id);
    if (!syncing) return;
    syncing.unsubscribe();
    this.syncing.delete(id);
  }
}

export type MemorySync1Adapter = Sync1Interface & {
  entities: Map<EntityIdStr, Entity>;
  subscribers: Map<EntityIdStr, Subscriber[]>;
};

export const memorySync1Adapters = (count = 2): MemorySync1Adapter[] => {
  const interfaces: MemorySync1Adapter[] = [];

  for (let i = 0; i < count; i++) {
    interfaces[i] = {
      entities: new Map(),
      subscribers: new Map(),
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
      subscribe(id, snapshot, handleUpdate) {
        const incoming = new LoroDoc();
        incoming.import(snapshot);
        const incomingFrontiers = incoming.frontiers();

        const entity = this.entities.get(id) || new Entity(id);
        entity.doc.import(snapshot);

        const subs: Subscriber[] = getOrDefault(this.subscribers, id, []);
        subs.push(handleUpdate);

        const cmp = entity.doc.cmpFrontiers(
          incomingFrontiers,
          entity.doc.frontiers()
        );
        if (cmp === -1 || cmp === undefined) {
          handleUpdate(
            id,
            entity.doc.export({ mode: "update", from: incoming.version() })
          );
        }

        this.entities.set(id, entity);

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

export type SuperPeer1Option = StorageConfig | StorageManager;

/**
 * A {@linkcode SuperPeer1} is a peer that accepts connections from any peer, syncs every
 * {@linkcode Entity} that it becomes aware of, and persists the entities to storage.
 *
 * It is meant to be run as a part of a sync server that can be used to persist and synchronize
 * {@linkcode Entity}s between multiple client peers, or possibly other super peers.
 */
export class SuperPeer1 implements Sync1Interface {
  #storages: StorageConfig[] = [];
  #subscribers: Map<EntityIdStr, Subscriber[]> = new Map();

  constructor(...options: SuperPeer1Option[]) {
    for (const option of options) {
      if (option instanceof StorageManager) {
        this.#storages.push({
          manager: option,
        });
      } else if ("manager" in option) {
        this.#storages.push(option);
      }
    }
  }

  subscribe(
    entityId: EntityIdStr,
    snapshot: Uint8Array,
    handleUpdate: Subscriber
  ): () => void {
    (async () => {
      try {
        const incomingEnt = new Entity(entityId);
        incomingEnt.doc.import(snapshot);

        const ent = new Entity(entityId);
        for (const storage of this.#storages) {
          if (storage.read !== false) {
            await storage.manager.load(ent);
          }
        }
        const currentFrontiers = ent.doc.frontiers();
        ent.doc.import(snapshot);
        const newFrontiers = ent.doc.frontiers();

        // If the sync gave us new data
        const cmp = ent.doc.cmpFrontiers(currentFrontiers, newFrontiers);
        if (cmp === -1 || cmp === undefined) {
          // Update storage
          for (const storage of this.#storages) {
            if (storage.write !== false) {
              storage.manager.save(ent);
            }
          }

          // And sync it to all subscribers
          const subscriberUpdate = ent.doc.export({
            mode: "update",
            from: ent.doc.frontiersToVV(currentFrontiers),
          });
          for (const sub of getOrDefault(this.#subscribers, entityId, [])) {
            sub(entityId, subscriberUpdate);
          }
        }

        // Return the updates that the peer didn't already have.
        handleUpdate(
          entityId,
          ent.doc.export({ mode: "update", from: incomingEnt.doc.version() })
        );
      } catch (e) {
        console.error(new Error(`Error syncing snapshots: ${e}`));
      }
    })();

    const subs = getOrDefault(this.#subscribers, entityId, []);
    subs.push(handleUpdate);

    return () => {
      const subs = getOrDefault(this.#subscribers, entityId, []);
      this.#subscribers.set(
        entityId,
        subs.filter((x) => x !== handleUpdate)
      );
    };
  }

  sendUpdate(entityId: EntityIdStr, update: Uint8Array): void {
    try {
      const ent = new Entity(entityId);
      // Load ent from storage
      for (const storage of this.#storages) {
        if (storage.read !== false) {
          storage.manager.load(ent);
        }
      }
      const currentFrontiers = ent.doc.frontiers();
      ent.doc.import(update);
      const newFrontiers = ent.doc.frontiers();

      // If the sync gave us new data
      if (ent.doc.cmpFrontiers(currentFrontiers, newFrontiers) == -1) {
        // Save to storage
        for (const storage of this.#storages) {
          if (storage.write !== false) {
            storage.manager.save(ent);
          }
        }

        // Notify subscribers
        for (const sub of getOrDefault(this.#subscribers, entityId, [])) {
          sub(entityId, update);
        }
      }
    } catch (e) {
      console.error(new Error(`Error syncing snapshots: ${e}`));
    }
  }
}
