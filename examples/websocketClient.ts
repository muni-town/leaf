/**
 * First run the websocket server before running this example: deno run syncserver
 *
 * @example
 * @module
 */

import { EntityId, EntityIdStr, Peer } from "../src/index.ts";
import { Age } from "./components.ts";
import { webSocketSyncer } from "../src/sync1/ws-client.ts";

const websocket = new WebSocket("ws://localhost:8095");
const peer1 = new Peer(await webSocketSyncer(websocket));

const entityId = new EntityId((Deno.args[0] as EntityIdStr) || undefined);

const ent1 = await peer1.open(entityId, { awaitSync: true });
console.log(`${ent1.id.toString()}`);

console.log("initial value", ent1.doc.toJSON());

ent1.getOrInit(Age).increment(1);
ent1.commit();

console.log("updated value", ent1.doc.toJSON());

// Note: We have to wait for the peer to close before we kill the websocket if we want our commit to
// get sent to the server before the client exits.
await peer1.close(ent1);
websocket.close();
