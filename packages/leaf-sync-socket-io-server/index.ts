/**
 * @module @muni-town/leaf-sync-socket-io-server-deno
 */

import { createServer, type Server as HttpServer } from "node:http";
import {
  StorageManager,
  SuperPeer1,
  SuperPeer1BinaryWrapper,
} from "@muni-town/leaf";
import { denoKvBlobStorageAdapter } from "@muni-town/leaf-storage-deno-kv";
import { Server as SocketIoServer } from "socket.io";

/**
 * Attach a super peer, socket IO server instance, and an http server, so that http requests will be
 * handled by socket IO and used to create a sync connection to the super peer.
 */
export function attachServer(
  superPeer: SuperPeer1,
  socketIoServer: SocketIoServer<
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

/** Start a Socket.io sync server */
export async function startServer(opts: { port: number; dbFile: string }) {
  const superPeer = new SuperPeer1(
    new StorageManager(denoKvBlobStorageAdapter(await Deno.openKv(opts.dbFile)))
  );

  const httpServer = createServer();
  const socketIoServer = new SocketIoServer({
    cors: {
      allowedHeaders: "*",
      origin: "*",
    },
  });
  attachServer(superPeer, socketIoServer, httpServer);
  httpServer.listen(opts.port);
  console.log(`Listening on http://0.0.0.0:${opts.port}`);
}

if (import.meta.main) {
  const port = parseInt(Deno.env.get("PORT") || "8000");
  const dbFile = Deno.env.get("DB_FILE") || "./data.sqlite";

  startServer({ port, dbFile });
}
