/**
 * @module @muni-town/leaf-storage-deno-indexeddb
 */


import type { StorageInterface } from "@muni-town/leaf";
import { IndexedDBStorageAdapter } from "@automerge/automerge-repo-storage-indexeddb";

export function indexedDBStorageAdapter(
  databaseName: string
): StorageInterface {
  return new IndexedDBStorageAdapter(databaseName);
}
