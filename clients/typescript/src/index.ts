import { io, Socket } from "socket.io-client";
import parser from "socket.io-msgpack-parser";
import {
  BasicModuleDef,
  Hash,
  HasModuleResp,
  IncomingEvent,
  LeafModuleCodec,
  LeafQuery,
  SqlRows,
  StreamEventBatchArgs,
  StreamGenesis,
  StreamInfo,
  StreamQueryArgs,
  StreamQueryResp,
  StreamSubscribeArgs,
  StreamSubscribeNotification,
  Ulid,
} from "./codec.js";
import { blake3 } from "@noble/hashes/blake3.js";
import { hex } from "@scure/base";
import { _void, bool, Result, str, Struct } from "scale-ts";

export * from "./codec.js";

// Helper to ensure binary data is in the correct format for socket.io-msgpack-parser
// In Node.js, the parser requires Buffer. In browsers, Uint8Array works.
function toBinary(data: Uint8Array): Uint8Array {
  // Check if Buffer is available (Node.js environment)
  if (typeof Buffer !== 'undefined' && Buffer.from) {
    return Buffer.from(data);
  }
  return data;
}

// Helper to convert response data back to plain Uint8Array for scale-ts decoding
// Buffer in Node.js can cause issues with scale-ts decoding
function fromBinary(data: Uint8Array): Uint8Array {
  // If it's a Buffer, convert to plain Uint8Array
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(data)) {
    return new Uint8Array(data);
  }
  return data;
}

type EventMap = {
  connect: () => void;
  disconnect: () => void;
  authenticated: (did?: string) => void;
  error: (error: string) => void;
};

export class LeafClient {
  socket: Socket;
  #querySubscriptions: Map<
    string,
    (result: StreamQueryResp) => void | Promise<void>
  > = new Map();
  _listeners: { [K in keyof EventMap]: EventMap[K][] } = {
    connect: [],
    disconnect: [],
    authenticated: [],
    error: [],
  };

  /**
   * The second arg is the authenticator which must obtain a valid ATProto service auth token:
   *
   * ```ts
   * const client = new LeafClient("http://localhost:5530", async () => {
   *   const data = await user.agent!.com.atproto.server.getServiceAuth({
   *     aud: "did:web:localhost:5530",
   *   });
   *   return data.data.token;
   * });
   * ```
   */
  constructor(url: string, authenticator?: () => Promise<string>) {
    this.socket = io(url, {
      parser,
      auth: (cb) => {
        if (authenticator) {
          authenticator().then((token) => {
            cb({ token });
          });
        } else {
          cb({ token: undefined });
        }
      },
    });

    this.socket.compress(true);
    this.socket.on("connect", () => {
      this.#emit("connect");
    });
    this.socket.on("disconnect", () => this.#emit("disconnect"));
    this.socket.on("authenticated", (data: { did?: string }) => {
      this.#emit("authenticated", data.did);
    });
    this.socket.on("error", (error) => {
      this.#emit("error", error);
    });
    this.socket.on("stream/subscription_response", (data: Uint8Array) => {
      const notification = StreamSubscribeNotification.dec(fromBinary(data));
      const sub = this.#querySubscriptions.get(notification.subscription_id);
      if (sub) {
        sub(notification.response);
      }
    });
  }

  #emit<E extends keyof EventMap>(event: E, ...args: Parameters<EventMap[E]>) {
    for (const listener of this._listeners[event]) {
      (listener as any)(...(args as []));
    }
  }

  on<E extends keyof EventMap>(event: E, handler: EventMap[E]) {
    this._listeners[event].push(handler as any);
  }

  off<E extends keyof EventMap>(event: E, handler: EventMap[E]) {
    this._listeners[event] = this._listeners[event].filter(
      (x) => x != handler,
    ) as any;
  }

  async uploadBasicModule(module: BasicModuleDef): Promise<string> {
    return this.uploadModule(
      toBinary(LeafClient.encodeBasicModule(module).encoded),
    );
  }

  static encodeBasicModule(module: BasicModuleDef): {
    encoded: Uint8Array;
    moduleId: string;
  } {
    let data = LeafModuleCodec.enc({
      moduleTypeId: "muni.town.leaf.module.basic.0",
      data: BasicModuleDef.enc(module),
    });
    let hash = blake3(data);
    let hashStr = hex.encode(hash);
    return {
      encoded: data,
      moduleId: hashStr,
    };
  }

  async uploadModule(module: Uint8Array | ArrayBufferLike): Promise<string> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "module/upload",
      module,
    );
    const resp = Result(Hash, str).dec(fromBinary(data));
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async hasModule(moduleId: string): Promise<boolean> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "module/exists",
      toBinary(Hash.enc(moduleId)),
    );
    const resp = HasModuleResp.dec(fromBinary(data));
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async createStream(genesis: StreamGenesis): Promise<string> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/create",
      toBinary(StreamGenesis.enc(genesis)),
    );
    const resp = Result(Hash, str).dec(fromBinary(data));
    if (!resp.success) {
      console.error(resp);
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async streamInfo(streamId: string): Promise<StreamInfo> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/info",
      toBinary(Struct({
        streamId: Hash,
      }).enc({ streamId })),
    );
    const resp = Result(StreamInfo, str).dec(fromBinary(data));
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async updateModule(streamId: string, moduleId: string): Promise<void> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/update_module",
      toBinary(Struct({
        streamId: Hash,
        moduleId: Hash,
      }).enc({ streamId, moduleId })),
    );
    const resp = Result(_void, str).dec(fromBinary(data));
    if (!resp.success) {
      console.error(resp);
      throw new Error(resp.value);
    }
    return resp.value;
  }

  /** Helper to create a stream from a WASM module at a given URL, avoiding uploading / downloading
   * the WASM if the module ID already exists on the server. */
  async createStreamFromModuleUrl(
    genesis: StreamGenesis,
    url: string,
  ): Promise<string> {
    const hasModule = await this.hasModule(genesis.module);

    if (!hasModule) {
      const resp = await fetch(url);
      const data = await resp.blob();
      const buffer = await data.arrayBuffer();
      const uploadedId = await this.uploadModule(buffer);
      if (uploadedId !== genesis.module)
        throw new Error(
          `The module ID that was uploaded didn't match the expected module ID. Expected ${genesis.module} got ${uploadedId}`,
        );
    }

    return await this.createStream(genesis);
  }

  async sendEvent(streamId: string, event: IncomingEvent): Promise<void> {
    this.sendEvents(streamId, [event]);
  }

  async sendEvents(streamId: string, events: IncomingEvent[]): Promise<void> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/event_batch",
      toBinary(StreamEventBatchArgs.enc({ streamId, events })),
    );
    const resp = Result(_void, str).dec(fromBinary(data));
    if (!resp.success) {
      throw new Error(resp.value);
    }
  }

  /** Returns a function that can be called to unsubscribe the query. */
  async subscribe(
    streamId: string,
    query: LeafQuery,
    handler: (resp: StreamQueryResp) => Promise<void> | void,
  ): Promise<() => Promise<void>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/subscribe",
      toBinary(StreamSubscribeArgs.enc({ streamId, query })),
    );
    const resp = Result(Ulid, str).dec(fromBinary(data));
    if (!resp.success) {
      throw new Error(resp.value);
    }

    const subId = resp.value;
    this.#querySubscriptions.set(subId, handler);
    return async () => {
      const data: Uint8Array = await this.socket.emitWithAck(
        "stream/unsubscribe",
        toBinary(Ulid.enc(subId)),
      );
      this.#querySubscriptions.delete(subId);
      const resp = Result(bool, str).dec(fromBinary(data));
      if (!resp.success) {
        throw new Error(`Error unsubscribing to query: ${resp.value}`);
      }
    };
  }

  disconnect() {
    this.socket.disconnect();
    for (const key in this._listeners) {
      this._listeners[key as keyof typeof this._listeners] = [];
    }
  }

  async query(streamId: string, query: LeafQuery): Promise<SqlRows> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/query",
      toBinary(StreamQueryArgs.enc({ streamId, query })),
    );
    const resp = StreamQueryResp.dec(fromBinary(data));
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }
}
