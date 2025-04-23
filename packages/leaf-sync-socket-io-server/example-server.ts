import { denoKvBlobStorageAdapter } from "@muni-town/leaf-storage-deno-kv";
import { attachServer } from "./index.ts";
import { SuperPeer1 } from "../leaf/sync1.ts";
import { StorageManager } from "../leaf/storage.ts";
import { createServer } from "node:http";
import { Server as SocketIoServer } from "socket.io";

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
