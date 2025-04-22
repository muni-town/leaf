/**
 * @module @muni-town/leaf-sync-socket-io-client
 */

import {
  Syncer1,
  type Sync1BinaryInterface,
  Sync1BinaryWrapper,
} from "@muni-town/leaf";
import { Socket, io } from "socket.io-client";

/**
 * A {@linkcode Sync1BinaryInterface} implementation on top of a {@linkcode WebSocket}.
 */
export class SocketIoClientBinaryInterface implements Sync1BinaryInterface {
  #socket: Socket<
    { data: (data: ArrayBuffer) => void },
    { data: (data: Uint8Array) => void }
  >;

  constructor(socket: Socket) {
    this.#socket = socket;
  }

  send(clientMessage: Uint8Array): Promise<void> {
    this.#socket.emit("data", clientMessage);
    return Promise.resolve();
  }
  setReceiver(receiver: (serverResponse: Uint8Array) => void): void {
    this.#socket.on("data", (buffer) => {
      const data = new Uint8Array(buffer);
      receiver(data);
    });
  }
}

/**
 * Create a {@linkcode Syncer1} that will sync with a {@linkcode SuperPeer1} over a websocket.
 * */
export function socketIoSyncer(
  ...options: Parameters<typeof io> | [Socket]
): Syncer1 {
  let socket: Socket;
  if (options[0] instanceof Socket) {
    socket = options[0];
  } else {
    socket = io(...(options as Parameters<typeof io>));
  }

  return new Syncer1(
    new Sync1BinaryWrapper(new SocketIoClientBinaryInterface(socket))
  );
}
