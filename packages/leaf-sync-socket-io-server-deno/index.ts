/**
 * Socket.io server implementation of a {@linkcode SuperPeer1}.
 *
 * This is a simple example module that can be used as a reference for making your own, customized
 * super peer sync servers.
 *
 * It uses Deno's HTTP server and Deno KV for super peer storage.
 *
 * You can use it by running this module directly with Deno, i.e. it can be used as the "main"
 * module, and it will start a server, reading the `PORT` and `DB_FILE` environment variables to
 * configure the HTTP port and path to the Sqlite database used for storage.
 *
 * You can also use the {@linkcode getHandler} function in your own Deno server if you want more
 * control over how the connections are opened or need to add, for example, authentication, or use
 * with your own {@linkcode SuperPeer1}.
 *
 * Finally you can use the {@linkcode startServer} function if you don't want to implement the
 * server yourself, but you would like to be able to start it from your own script.
 *
 * This whole module is tiny and meant to stay simple and serve as a useful reference. Most people
 * will probably want to use it as an example and make something custom.
 *
 * @module @muni-town/leaf-sync-socket-io-server-deno
 */

import {
  StorageManager,
  SuperPeer1,
  SuperPeer1BinaryWrapper,
} from "@muni-town/leaf";
import { denoKvBlobStorageAdapter } from "@muni-town/leaf-storage-deno-kv";
import { Server } from "socket-io-server";

/**
 * Handle an HTTP request, upgrading it to a websocket connection, and hosting the super peer binary
 * protocol over it.
 */
export function getHandler(
  superPeer: SuperPeer1,
  server: Server<
    { data: (data: ArrayBuffer) => void },
    { data: (data: Uint8Array) => void }
  >
): (req: Request, opts: Deno.ServeHandlerInfo) => Promise<Response> {
  server.on("connection", (socket) => {
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

  const handler = server.handler();
  return async (req, info) => {
    const resp = await handler(req, info);
    return resp;
  };
}

/** Start a websocket sync server */
export async function startServer(opts: { port: number; dbFile: string }) {
  const superPeer = new SuperPeer1(
    new StorageManager(denoKvBlobStorageAdapter(await Deno.openKv(opts.dbFile)))
  );

  Deno.serve(
    { port: opts.port },
    getHandler(
      superPeer,
      new Server({
        cors: {
          allowedHeaders: "*",
          origin: "*",
        },
      })
    )
  );
}

if (import.meta.main) {
  const port = parseInt(Deno.env.get("PORT") || "8000");
  const dbFile = Deno.env.get("DB_FILE") || "./data.sqlite";

  startServer({ port, dbFile });
}
