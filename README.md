# Leaf Framework

[![](https://img.shields.io/badge/Docs-API-orange)](https://muni-town.github.io/leaf)

Welcome to the Leaf SDK! Leaf is a work-in-progress, opinionated wrapper around CRDT tools for building local-first apps!

> [!WARNING]  
> Leaf is extremely work-in-progress. We are still iterating, experimenting, and figuring out what Leaf will turn into long term. For now it's not ready for most people to start using it, but if you'd like to get involved or ask questions feel free to join our [Discord](https://discord.gg/BkEpMzzh38) or [Matrix](https://matrix.to/#/%23muni-town:commune.sh) chat.

## Core Principles

Leaf is trying to become a framework for app development that focuses on the following principles:

- **Local First:** applications should work out-of-the box offline. Network connectivity is an enhancement, but is completely optional.
- **Interoperable:** applications using the Leaf framework should have the possibility of interacting with each-others' data when the user grants the required access. This does not mean that all apps will be compatible with each-other, but we want to make it as easy as possible to integrate as little or as much as desired.
- **Great Developer Experience:** We want Leaf to be very easy to use. We think that the guiding principles of local first and interoperability are incredibly beneficial for users, but if it's hard for developers to make local-first, interoperable apps, then user's will never have the chance to benefit from it.

## Core Technology

We don't want to invent anything that we don't have to; we are going to try building on existing technology as much as we can while still accomplishing our goals. To that end we are planning on building on the following core technology:

- **[Loro](https://loro.dev/)** CRDT: Conflict-free replicated data type ( CRDT ) library.
- **[Keyhive](https://github.com/inkandswitch/keyhive):** Keyhive is a peer-to-peer ( and peer-to-"server" ), end-to-end encrypted sync protocol for commit-based DAGs ( like CRDTs ) that includes a capability & groups system.
- **[Sedimentree](https://github.com/inkandswitch/keyhive/blob/main/design/sedimentree.md):** Deterministic, chunked storage algorithm, used by Keyhive. We also plan on using it for incremental archives of Leaf data.

## Development

Getting started:

```bash
pnpm build # Compile the packages
```

### Running the Example

The examples are written in TypeScript and can be run like:

```bash
npx tsx examples/basicPeer.ts
```
