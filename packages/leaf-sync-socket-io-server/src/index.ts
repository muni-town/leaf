/**
 * @module @muni-town/leaf-sync-socket-io-server-deno
 */

import type { Server as HttpServer } from "node:http";
import { type SuperPeer1, SuperPeer1BinaryWrapper } from "@muni-town/leaf";

import type * as io from "socket.io";
export * as io from "socket.io";

/**
 * Attach a super peer, socket IO server instance, and an http server, so that http requests will be
 * handled by socket IO and used to create a sync connection to the super peer.
 */
export function attachServer(
  superPeer: SuperPeer1,
  socketIoServer: io.Server<
    { data: (data: ArrayBuffer) => void },
    { data: (data: Uint8Array) => void }
  >,
  httpServer: HttpServer
) {
  socketIoServer.on("connection", (socket) => {
    const wrapper = new SuperPeer1BinaryWrapper(superPeer);

    wrapper.setReceiver((data) => {
      socket.emit("data", data);
    });

    socket.on("data", (buffer) => {
      const data = new Uint8Array(buffer);
      wrapper.send(data);
    });

    socket.on("disconnect", () => wrapper.cleanup());
  });

  socketIoServer.attach(httpServer);
}
