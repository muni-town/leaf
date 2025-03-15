import { Entity, IntoEntityId, Peer, PeerOpenOptions } from "./leaf.ts";
import { createSubscriber } from "svelte/reactivity";

/**
 * Drop-in replacement for {@linkcode Peer} that will return entities that are reactive in Svelte
 * contexts.
 *
 * Usually you will use this instead of {@linkcode Peer} when using entities in Svelte.
 *
 * Alternatively you can use {@linkcode reactiveEntity} manually to wrap a non-rective
 * {@linkcode Entity} in a reactive proxy.
 * */
export class SveltePeer extends Peer {
  // Override the entity open function
  override async open(
    id?: IntoEntityId,
    opts?: Partial<PeerOpenOptions>
  ): Promise<Entity> {
    // Get the entity like normal
    const entity = await super.open(id, opts);
    // And wrap it in a reactive entity
    return reactiveEntity(entity);
  }
}

/** Take an entity and return a reactive version of it. */
export function reactiveEntity(entity: Entity): Entity {
  // Create a subscriber for the entity
  const subscribe = createSubscriber((update) => {
    entity.doc.subscribe(() => {
      // Re-render when the doc changes.
      update();
    });
  });

  // Return a proxied entity that will subscribe to changes whenever an entity field is accessed.
  return new Proxy(entity, {
    get(target, prop) {
      // NOTE: subscribing to gets on all  properties is not strictly necessary, but it's the
      // easiest way to catch everything. It _might_ be worth making this more specific so that it
      // doesn't trigger unnecessary subscriptions, but I'm not sure, because then it has to be kept
      // up-to-date with changes to the Entity API.
      subscribe();
      if (prop in target) {
        // deno-lint-ignore no-explicit-any
        const value = (target as any)[prop];
        return typeof value === "function" ? value.bind(target) : value;
      }
    },
  });
}
