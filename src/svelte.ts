/**
 * A svelte reactivity integration for Leaf. See {@linkcode SveltePeer} for usage.
 * 
 * @module
 */

import { Entity, IntoEntityId, Peer, PeerOpenOptions } from "./index.ts";
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

function makeProxy<T>(t: T, subscribe: () => void): T {
  if (typeof t !== "object") return t;
  return new Proxy(t as object, {
    get(target, prop) {
      subscribe();

      if (prop in target) {
        // deno-lint-ignore no-explicit-any
        const value = (target as any)[prop];

        // Recursively create wrap accessed types in a proxy
        return typeof value === "function"
          ? // deno-lint-ignore no-explicit-any
            (...args: any[]) => {
              const result = value.bind(target)(...args);
              return makeProxy(result, subscribe);
            }
          : makeProxy(value, subscribe);
      }
    },
  }) as T;
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
  return makeProxy(entity, subscribe);
}
