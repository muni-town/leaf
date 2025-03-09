/**
 * Storage related implementations and interfaces.
 *
 * @module
 */

import encodeBase32 from "base32-encode";
import { Frontiers } from "loro-crdt";
import { EntityIdStr, Entity } from "./leaf.ts";

function getOrDefault<K, V>(map: Map<K, V>, key: K, defaultValue: V): V {
  if (!map.has(key)) {
    map.set(key, defaultValue);
  }
  return map.get(key)!;
}

/** Get the sha256 hash of the given data. */
async function sha256Base32(data: Uint8Array): Promise<string> {
  return encodeBase32(
    new Uint8Array(await crypto.subtle.digest("SHA-256", new Uint8Array(data))),
    "Crockford"
  );
}

/** The key to a record in the {@linkcode StorageInterface}. */
export type StorageKey = string[];

/** The backend storage interfaced used by {@linkcode StorageManager}. */
export interface StorageInterface {
  /** Load the bytes at a given key. */
  load(key: StorageKey): Promise<Uint8Array | undefined>;
  /** Save the given data to the provided key. */
  save(key: StorageKey, data: Uint8Array): Promise<void>;
  /** Remove the data at a given key. */
  remove(key: StorageKey): Promise<void>;
  /** Load all values and keys that are prefixed by the given key. */
  loadRange(
    key: StorageKey
  ): Promise<{ key: StorageKey; data: Uint8Array | undefined }[]>;
  /** Remove all values that are prefixed by the given key. */
  removeRange(key: StorageKey): Promise<void>;
}

type ChunkInfo = {
  kind: "snapshot" | "incremental";
  hash: string;
  size: number;
};
type Chunk = { data: Uint8Array } & ChunkInfo;

const dataPrefix = "data";

/**
 * Storage interface for saving and loading {@linkcode Entity}s from a {@linkcode StorageInterface}.
 *
 * The storage manager keeps track of the latest snapshots that have been loaded from storage so
 * that it can delete old snapshots and update storage even in the presence of multiple concurrent
 * readers / writers to the same data store.
 */
export class StorageManager {
  /** The backing storage interface for this storage manager. */
  storage: StorageInterface;

  /** Construct a new {@linkcode StorageManager} for the given {@linkcode StorageInterface}. */
  constructor(storage: StorageInterface) {
    this.storage = storage;
  }

  /** The chunks that we most-recently loaded from storage. */
  loadedChunks: Map<EntityIdStr, ChunkInfo[]> = new Map();
  /** The heads that we most-recently loaded from storage. */
  loadedFrontiers: Map<EntityIdStr, Frontiers> = new Map();

  /**
   * Load an entity from storage.
   *
   * If there is no data for the entity in storage this will leave the entity un-modified.
   *
   * If there is data in storage, it will be loaded and merged into this entity.
   *
   * @param entity The entity to load data into.
   * @returns Returns `true` if the entity was found in storage and `false` if it was not.
   */
  async load(entity: Entity): Promise<boolean> {
    const id = entity.id.toString();

    // Load chunks from storage
    const chunks: Chunk[] = (await this.storage.loadRange([dataPrefix, id]))
      .filter((x) => !!x.data)
      .map(({ key, data }) => {
        return {
          kind: key[key.length - 2] == "snapshot" ? "snapshot" : "incremental",
          hash: key[key.length - 1],
          size: data!.length,
          data: data!,
        };
      });

    // If there is no data, just return the old doc;
    if (chunks.length == 0) return false;

    // Load the doc data from storage
    entity.doc.importBatch(chunks.map((x) => x.data));

    // Update loaded chunks & heads
    this.loadedChunks.set(
      id,
      chunks.map((x) => ({ ...x, data: undefined }))
    );
    this.loadedFrontiers.set(id, entity.doc.frontiers());

    // The do loaded
    return true;
  }

  /**
   * Persist an entity to storage.
   *
   * @param entity The entity to save.
   */
  async save(entity: Entity) {
    const id = entity.id.toString();
    // NOTE: copying the loaded chunks, immediately at the time of triggering the save, is very
    // important to make sure that a concurrent call to loadFromStorage doesn't change the loaded
    // chunks and cause us to delete chunks that we have just loaded and not included in the data
    // being saved in this function.
    let toDelete = [...getOrDefault(this.loadedChunks, id, [])];

    // Get the heads of the current version of the document
    const frontiers = entity.doc.frontiers();

    // Check whether the current version is newer than the last version we loaded from storage.
    const needsSave =
      entity.doc.cmpWithFrontiers(
        getOrDefault(this.loadedFrontiers, id, [])
      ) !== 0;

    // Skip saving if it's already up-to-date
    if (!needsSave) {
      return;
    }

    // Export and hash the document
    const binary = entity.doc.export({ mode: "snapshot" });

    // TODO: we could hash the frontiers instead, but we need a way to deterministically encode the
    // frontiers first.
    const hash = await sha256Base32(binary);

    // Just in case, make sure we don't delete the file we're about to create.
    toDelete = toDelete.filter((x) => x.hash !== hash);

    // Safe the document to storage
    await this.storage.save([dataPrefix, id, "snapshot", hash], binary);

    // Delete previously loaded chunks that we no longer need from storage
    for (const chunk of toDelete) {
      await this.storage!.remove([dataPrefix, id, chunk.kind, chunk.hash]);
    }

    // Update our list of loaded chunks to the snapshot we just exported.
    this.loadedChunks.set(id, [
      {
        hash,
        kind: "snapshot",
        size: binary.length,
      },
    ]);
    this.loadedFrontiers.set(id, frontiers);
  }
}

/** Takes another {@linkcode StorageInterface} and creates a sub-interface with the given key namespace. */
export function namespacedSubstorage(
  storage: StorageInterface,
  ...namespace: string[]
): StorageInterface {
  return {
    load(key) {
      return storage.load([...namespace, ...key]);
    },
    async loadRange(key) {
      const result = await storage.loadRange([...namespace, ...key]);
      return result.map((x) => ({
        key: x.key.slice(namespace.length),
        data: x.data,
      }));
    },
    remove(key) {
      return storage.remove([...namespace, ...key]);
    },
    removeRange(key) {
      return storage.removeRange([...namespace, ...key]);
    },
    save(key, value) {
      return storage.save([...namespace, ...key], value);
    },
  };
}
