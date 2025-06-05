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

export * from "loro-crdt";
export * from "./sync1.ts";
export * from "./storage.ts";
export * from "./sync-proto.ts";
export * from "./entity.ts";
export * from "./component.ts";
export * from "./peer.ts";
