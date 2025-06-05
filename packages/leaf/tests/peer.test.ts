import { describe, it, expect, beforeEach, afterEach } from "vitest";
import {
  Entity,
  Peer,
  defComponent,
  LoroMap,
  IntoEntityId,
  intoEntityId,
} from "../src";
import { StorageManager, StorageInterface } from "../src/storage";
import { Syncer1 } from "../src/sync1";

describe("Peer", () => {
  // Test components
  const Name = defComponent(
    "name:01JNVY76XPH6Q5AVA385HP04G7",
    LoroMap<{ first: string; last?: string }>,
    (map) => map.set("first", "unnamed")
  );

  // Mock storage interface
  class MockStorageInterface implements StorageInterface {
    private store = new Map<string, Uint8Array>();

    async load(key: string[]): Promise<Uint8Array | undefined> {
      return this.store.get(key.join("/"));
    }

    async save(key: string[], data: Uint8Array): Promise<void> {
      this.store.set(key.join("/"), data);
    }

    async delete(key: string[]): Promise<void> {
      this.store.delete(key.join("/"));
    }

    async remove(key: string[]): Promise<void> {
      this.store.delete(key.join("/"));
    }

    async loadRange(
      key: string[]
    ): Promise<{ key: string[]; data: Uint8Array | undefined }[]> {
      const prefix = key.join("/");
      const results: { key: string[]; data: Uint8Array | undefined }[] = [];
      for (const [k, v] of this.store.entries()) {
        if (k.startsWith(prefix)) {
          results.push({
            key: k.split("/"),
            data: v,
          });
        }
      }
      return results;
    }

    async deleteRange(key: string[]): Promise<void> {
      const prefix = key.join("/");
      for (const k of this.store.keys()) {
        if (k.startsWith(prefix)) {
          this.store.delete(k);
        }
      }
    }

    async removeRange(key: string[]): Promise<void> {
      const prefix = key.join("/");
      for (const k of this.store.keys()) {
        if (k.startsWith(prefix)) {
          this.store.delete(k);
        }
      }
    }
  }

  // Mock storage manager
  class MockStorageManager extends StorageManager {
    constructor() {
      super(new MockStorageInterface());
    }
  }

  // Mock syncer
  class MockSyncer extends Syncer1 {
    private updates = new Map<string, Uint8Array>();

    constructor() {
      super({
        subscribe: (entityId, versionVector, handleUpdate) => {
          const update = this.updates.get(entityId);
          if (update) {
            handleUpdate(entityId, update);
          }
          return () => {};
        },
        sendUpdate: (entityId, update) => {
          this.updates.set(entityId, update);
        },
      } as any); // Type assertion needed due to interface mismatch
    }
  }

  let peer: Peer;
  let storage: MockStorageManager;
  let syncer: MockSyncer;
  let openEntities: Entity[] = [];

  beforeEach(() => {
    storage = new MockStorageManager();
    syncer = new MockSyncer();
    peer = new Peer(storage, syncer);
    openEntities = [];
  });

  afterEach(async () => {
    // Clean up any open entities
    for (const entity of openEntities) {
      try {
        await peer.close(entity.id);
      } catch (e) {
        console.warn("Error closing entity:", e);
      }
    }
    openEntities = [];
  });

  it("should create a new entity", async () => {
    const entity = await peer.create();
    openEntities.push(entity);
    expect(entity).toBeDefined();
    expect(entity.id).toBeDefined();
  });

  it("should open an existing entity", async () => {
    // Create and save an entity
    const entity = await peer.create();
    openEntities.push(entity);
    entity.init(Name);
    entity.getOrInit(Name, (name) => {
      name.set("first", "John");
      return name;
    });
    await peer.close(entity.id);
    openEntities = openEntities.filter((e) => e.id !== entity.id);

    // Open the same entity
    const reopened = await peer.open(entity.id);
    openEntities.push(reopened);
    expect(reopened.id.toString()).toBe(entity.id.toString());

    reopened.getOrInit(Name, (name) => {
      expect(name.get("first")).toBe("John");
      return name;
    });
  });

  it("should sync changes between peers", async () => {
    const entity = await peer.create();
    openEntities.push(entity);
    entity.init(Name);

    // Make changes
    entity.getOrInit(Name, (name) => {
      name.set("first", "John");
      return name;
    });

    // Create a second peer with the same storage and syncer
    const peer2 = new Peer(storage, syncer);
    const entity2 = await peer2.open(entity.id);
    openEntities.push(entity2);

    // Verify changes are synced
    entity2.getOrInit(Name, (name) => {
      expect(name.get("first")).toBe("John");
      return name;
    });

    await peer2.close(entity2.id);
    openEntities = openEntities.filter((e) => e.id !== entity2.id);
  }, 10000); // Increase timeout for sync test

  it("should handle entity deletion", async () => {
    const entity = await peer.create();
    openEntities.push(entity);
    const id = entity.id;

    // Save some data
    entity.init(Name);
    entity.getOrInit(Name, (name) => {
      name.set("first", "John");
      return name;
    });

    // Delete the entity
    await peer.delete(id);
    openEntities = openEntities.filter((e) => e.id !== id);

    // Try to open it again
    const reopened = await peer.open(id);
    openEntities.push(reopened);
    reopened.getOrInit(Name, (name) => {
      expect(name.get("first")).toBe("unnamed"); // Should have default value
      return name;
    });
  }, 10000); // Increase timeout for deletion test

  it("should handle concurrent modifications", async () => {
    const entity = await peer.create();
    openEntities.push(entity);
    entity.init(Name);

    // Create two peers
    const peer2 = new Peer(storage, syncer);
    const peer3 = new Peer(storage, syncer);

    const entity2 = await peer2.open(entity.id);
    const entity3 = await peer3.open(entity.id);
    openEntities.push(entity2, entity3);

    // Make concurrent modifications
    entity2.getOrInit(Name, (name) => {
      name.set("first", "John");
      return name;
    });

    entity3.getOrInit(Name, (name) => {
      name.set("last", "Doe");
      return name;
    });

    // Wait for sync
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Verify both changes are present
    entity.getOrInit(Name, (name) => {
      expect(name.get("first")).toBe("John");
      expect(name.get("last")).toBe("Doe");
      return name;
    });

    await peer2.close(entity2.id);
    await peer3.close(entity3.id);
    openEntities = openEntities.filter(
      (e) => e.id !== entity2.id && e.id !== entity3.id
    );
  }, 10000); // Increase timeout for concurrent modifications test
});
