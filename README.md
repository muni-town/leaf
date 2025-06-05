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

## Entity-Component Model

A core part of our strategy for interoperability is the use of an Entity-Component data model. The concept is that every atomic, sharable "thing", from the perspective of the capability / permissions system, is called an "entity", and all the data for that entity is made up of a set of "components". Each component has its own schema that defines the expected / valid data for that component.

By breaking the data for entities into components such `Name`, `Description`, and `Image`, we allow apps to _incrementally understand_ an entity, based on the components that it understands. Whether an entity is a blog post, a wiki page, or even a user profile, website, or organization, all those entities will most-likely have a `Name`, `Description`, and maybe an `Image`. This allows applications to display a preview for that entity, regardless of the fact that it may not know how to edit the entity, or display a richer view of it.

By allowing applications to unerstand entities through components we allow for a level of interoperability that is very extensible, since every app can make its own components when necessary, but at the same time a component ecosystem can develop where many apps use the same standard components for certain things like `Name` and `Description`, so that the apps can have at least a minimal level of inter-operability, and the more apps that converge on the same components, the better they inter-op with each-other.

## Planned Applications

Leaf is the culmination of our core stack for building our own applications. We are splitting it out from our applications so that we can more easily build more applications with it and allow other people to use it, too. We aren't just focusing on building a framework, we are allowing the framework to grow out of the real-world needs of our applications.

We at the Muni Town org are currently planning on buiding the [Roomy](https://github.com/muni-town/roomy) and [Weird](https://github.com/muni-town/weird) applications on top of Leaf, but since the applications are in development now and Leaf and its core technology like Keyhive are not read for use yet, Roomy and Weird are being developed on temporary alternatives.

The plan is to migrate both apps to Leaf as soon as it is ready to take the load.

## Further Reading

The ideas for Leaf have developed and changed a lot since its initial conception. It will probably continue to change a lot, but it can be helpful to have some more background on where it's come from and how it's developed. The following articles were written as we worked on Leaf. Most are out-of-date in some way but many of the ideas and motivation remain, and some of the ideas are sitll a part of future plans. These are ordered roughly by current relevance, not chronologically.

Most of these were written before Keyhive was a part of the plan, and we were thinking of building on top of [Willow](https://willowprotocol.org/) instead.

- **[A Web of Data](https://zicklag.katharos.group/blog/a-web-of-data/)**: This was the first time the entity-component model was considered as a means for interoperability over data.
- **[Capability & Identity with Leaf](https://blog.muni.town/capabilities-and-identity-with-leaf/#frost-signatures-2fa-for-keypairs)**: This goes over some of the ideas behind identity management with a capability system, including some thoughts on how to accomplish something like 2FA with keypairs.
- **[Leaf, AtProto, and ActivityPub](https://blog.muni.town/leaf-atproto-activitypub/)**: Exploration of the similarities and differences betweeen Leaf, AtProto, and ActivityPub.
- **[Introducing the Leaf Protocol](https://zicklag.katharos.group/blog/introducing-leaf-protocol/)**: An out-of-date introduction to our ideas for the Leaf protocol / framework.

We are due for a more up-to-date overview of our plans for Leaf, but we haven't published one yet.

## Development

Getting started:

```bash
pnpm build # Compile the packages
```

This repository uses pnpm with [JSR packages](https://jsr.io/docs/with/node), which work best with pnpm version 10.9 or higher.

### Running the Example

The examples are written in TypeScript and can be run like:

```bash
npx tsx examples/basicPeer.ts
```
