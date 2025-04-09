/**
 * WebSocket server implementation of a {@linkcode SuperPeer1}.
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
 * You can also use the {@linkcode handleRequest} function in your own Deno server if you want more
 * control over how the connections are opened or need to add, for example, authentication, or use
 * with your own {@linkcode SuperPeer1}.
 *
 * Finally you can use the {@linkcode startServer} function if you don't want to implement the
 * server yourself, but you would like to be able to start it from your own script.
 *
 * This whole module is tiny and meant to stay simple and serve as a useful reference. Most people
 * will probably want to use it as an example and make something custom.
 *
 * @module
 */

import { StorageManager } from "../storage.ts";
import { denoKvToolboxStorageAdapter } from "../storage/deno-kv-toolbox.ts";
import { SuperPeer1 } from "../sync1.ts";
import { SuperPeer1BinaryWrapper } from "./proto.ts";

/**
 * Handle an HTTP request, upgrading it to a websocket connection, and hosting the super peer binary
 * protocol over it.
 */
export function handleRequest(
  superPeer: SuperPeer1,
  request: Request
): Response {
  const { socket, response } = Deno.upgradeWebSocket(request);
  socket.binaryType = "arraybuffer";
  const wrapper = new SuperPeer1BinaryWrapper(superPeer);

  socket.addEventListener("open", () => {
    wrapper.setReceiver((data) => {
      socket.send(data);
    });
    let hasSentIngoredTxtMessage = false;
    socket.addEventListener("message", (ev) => {
      if (ev.data instanceof ArrayBuffer) {
        wrapper.send(new Uint8Array(ev.data));
      } else {
        if (!hasSentIngoredTxtMessage) {
          socket.send(
            `This is a Leaf sync server. WebSocket messages should be binary. Text messages will be ignored.`
          );
          hasSentIngoredTxtMessage = true;
        }
      }
    });
    socket.addEventListener("close", () => {
      wrapper.cleanup();
    });
  });

  return response;
}

/** Start a websocket sync server */
export async function startServer(opts: { port: number; dbFile: string }) {
  const superPeer = new SuperPeer1(
    new StorageManager(
      denoKvToolboxStorageAdapter(await Deno.openKv(opts.dbFile))
    )
  );

  Deno.serve({ port: opts.port }, (req) => {
    return handleRequest(superPeer, req);
  });
}

if (import.meta.main) {
  console.log("port", Deno.env.get("PORT"));
  const port = parseInt(Deno.env.get("PORT") || "8000");
  const dbFile = Deno.env.get("DB_FILE") || "./data.sqlite";

  startServer({ port, dbFile });
}
