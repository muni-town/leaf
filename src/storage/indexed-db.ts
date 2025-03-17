import { StorageInterface } from "../storage.ts";
import { IndexedDBStorageAdapter } from "@automerge/automerge-repo-storage-indexeddb";

export function indexedDBStorageAdapter(
  databaseName: string
): StorageInterface {
  return new IndexedDBStorageAdapter(databaseName);
}
