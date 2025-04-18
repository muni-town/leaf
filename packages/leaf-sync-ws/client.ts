/**
 * @module @muni-town/leaf-sync-ws
 */

import { Syncer1 } from "@muni-town/leaf";
import {
  type Sync1BinaryInterface,
  Sync1BinaryWrapper,
} from "../leaf/sync-proto.ts";

/**
 * A {@linkcode Sync1BinaryInterface} implementation on top of a {@linkcode WebSocket}.
 */
export class WebSocketClientBinaryInterface implements Sync1BinaryInterface {
  #socket: WebSocket;

  constructor(socket: WebSocket) {
    this.#socket = socket;
    this.#socket.binaryType = "arraybuffer";
  }

  send(clientMessage: Uint8Array): Promise<void> {
    this.#socket.send(clientMessage);
    return Promise.resolve();
  }
  setReceiver(receiver: (serverResponse: Uint8Array) => void): void {
    this.#socket.onmessage = (ev) => {
      if (ev.data instanceof ArrayBuffer) {
        receiver(new Uint8Array(ev.data));
      }
    };
  }
}

/**
 * Create a {@linkcode Syncer1} that will sync with a {@linkcode SuperPeer1} over a websocket.
 * */
export async function webSocketSyncer(websocket: WebSocket): Promise<Syncer1> {
  if (websocket.readyState != WebSocket.OPEN) {
    await new Promise((resolve) => websocket.addEventListener("open", resolve));
  }
  return new Syncer1(
    new Sync1BinaryWrapper(new WebSocketClientBinaryInterface(websocket))
  );
}
