/**
 * Sync1 protocol utilities and and types.
 *
 * Basically encodes the {@linkcode Sync1Interface} in a way that can be sent over a bidirectional
 * network channel.
 *
 * @module
 */

import { encode, decode } from "@msgpack/msgpack";
import { type } from "arktype";
import { EntityIdStr } from "../leaf.ts";
import { Subscriber, Sync1Interface, SuperPeer1 } from "../sync1.ts";
import { getOrDefault } from "../utils.ts";

/**
 * Binary interface that can wrapped by a {@linkcode Sync1BinaryWrapper} to produce a
 * {@linkcode Sync1Interface} that can be given to {@linkcode Syncer1}.
 * */
export interface Sync1BinaryInterface {
  /** Send a message to the server. */
  send(clientMessage: Uint8Array): Promise<void>;
  /** Register a callback that will be triggered when a message comes in from the server. */
  setReceiver(receiver: (serverResponse: Uint8Array) => void): void;
}

/**
 * Wraps a {@linkcode Sync1BinaryInterface} that implements the {@linkcode Sync1Interface} needed
 * by {@linkcode Syncer1}.
 * */
export class Sync1BinaryWrapper implements Sync1Interface {
  #binaryInterface: Sync1BinaryInterface;
  #subscribers: Map<EntityIdStr, Subscriber[]> = new Map();

  constructor(binaryInterface: Sync1BinaryInterface) {
    this.#binaryInterface = binaryInterface;
    binaryInterface.setReceiver(this.#receive.bind(this));
  }

  #receive(msg: Uint8Array) {
    const data = decodeServerResponse(msg);
    if (data.type == "handleUpdate") {
      for (const sub of getOrDefault(this.#subscribers, data.entityId, [])) {
        sub(data.entityId, data.update);
      }
    }
  }

  subscribe(
    entityId: EntityIdStr,
    snapshot: Uint8Array,
    handleUpdate: Subscriber
  ): () => void {
    const subs = getOrDefault(this.#subscribers, entityId, []);
    subs.push(handleUpdate);

    this.#binaryInterface.send(
      encodeClientMessage({
        type: "subscribe",
        snapshot,
        entityId,
      })
    );

    return () => {
      this.#subscribers.set(
        entityId,
        subs.filter((x) => x !== handleUpdate)
      );
      this.#binaryInterface.send(
        encodeClientMessage({ type: "unsubscribe", entityId })
      );
    };
  }

  sendUpdate(entityId: EntityIdStr, update: Uint8Array): void {
    this.#binaryInterface.send(
      encodeClientMessage({ type: "sendUpdate", entityId, update })
    );
  }
}

/**
 * Wraps a {@linkcode SuperPeer1} to provide a {@linkcode Sync1BinaryInterface}.
 *
 * This makes makes it easier to sync across binary network transports such as TCP or WebSockets.
 *
 * You should create a new {@linkcode SuperPeer1BinaryWrapper} around the same
 * {@linkcode SuperPeer1} for every client connection.
 * */
export class SuperPeer1BinaryWrapper implements Sync1BinaryInterface {
  #superPeer: SuperPeer1;
  #unsubscribers: Map<EntityIdStr, () => void> = new Map();
  #receiver: (serverResponse: Uint8Array) => void = () => {};

  constructor(superPeer: SuperPeer1) {
    this.#superPeer = superPeer;
  }

  send(clientMessage: Uint8Array): Promise<void> {
    const data = decodeClientMessage(clientMessage);
    if (!data) return Promise.resolve();

    if (data.type == "sendUpdate") {
      this.#superPeer.sendUpdate(data.entityId, data.update);
    } else if (data.type == "subscribe") {
      const unsubscribe = this.#superPeer.subscribe(
        data.entityId,
        data.snapshot,
        (entityId, update) => {
          this.#receiver(
            encodeServerResponse({
              type: "handleUpdate",
              update: update as Uint8Array<ArrayBuffer>,
              entityId,
            })
          );
        }
      );
      this.#unsubscribers.set(data.entityId, unsubscribe);
    } else if (data.type == "unsubscribe") {
      getOrDefault(this.#unsubscribers, data.entityId, () => {})();
    }

    return Promise.resolve();
  }

  setReceiver(receiver: (serverResponse: Uint8Array) => void): void {
    this.#receiver = receiver;
  }

  /** Should be called to unsubscribe all of the subscriptions made through this wrapper when you
   * are done with it. */
  cleanup() {
    for (const unsubscribe of this.#unsubscribers.values()) {
      unsubscribe();
    }
  }
}

/** Type verifier for client messages. */
export const clientMessage = type({
  type: "'unsubscribe'",
  entityId: "string" as type.cast<EntityIdStr>,
})
  .or({
    type: "'subscribe'",
    entityId: "string" as type.cast<EntityIdStr>,
    snapshot: type.instanceOf(Uint8Array<ArrayBufferLike>),
  })
  .or({
    type: "'sendUpdate'",
    entityId: "string" as type.cast<EntityIdStr>,
    update: type.instanceOf(Uint8Array<ArrayBufferLike>),
  });
/** The type of messages sent from the client to the server. */
export type ClientMessage = typeof clientMessage.infer;

/** Encode a client message to binary. */
export function encodeClientMessage(msg: ClientMessage): Uint8Array {
  return encode(msg);
}
/** Decode a binary client message. */
export function decodeClientMessage(
  msg: Uint8Array
): ClientMessage | undefined {
  const data = clientMessage(decode(msg));
  if (data instanceof type.errors) {
    console.error(data);
    return;
  }
  return data;
}

/** Type verifier for server responses. */
export const serverResponse = type({
  type: "'handleUpdate'",
  entityId: "string" as type.cast<EntityIdStr>,
  update: type.instanceOf(Uint8Array),
});
/** A message sent from the server to the client. */
export type ServerResponse = typeof serverResponse.infer;

/** Encode a server response to binary. */
export function encodeServerResponse(msg: ServerResponse): Uint8Array {
  return encode(msg);
}
/** Decode binary into aserver response */
export function decodeServerResponse(msg: Uint8Array): ServerResponse {
  const data = serverResponse(decode(msg));
  if (data instanceof type.errors) throw data;
  return data;
}
