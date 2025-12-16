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
      async auth(cb) {
        const token = authenticator ? await authenticator() : undefined;
        cb({ token });
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
      const notification = StreamSubscribeNotification.dec(data);
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
      LeafClient.encodeBasicModule(module).encoded.buffer,
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

  async uploadModule(module: ArrayBufferLike): Promise<string> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "module/upload",
      module,
    );
    const resp = Result(Hash, str).dec(data);
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async hasModule(moduleId: string): Promise<boolean> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "module/exists",
      Hash.enc(moduleId).buffer,
    );
    const resp = HasModuleResp.dec(data);
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async createStream(genesis: StreamGenesis): Promise<string> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/create",
      StreamGenesis.enc(genesis).buffer,
    );
    const resp = Result(Hash, str).dec(data);
    if (!resp.success) {
      console.error(resp);
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async streamInfo(streamId: string): Promise<StreamInfo> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/info",
      Struct({
        streamId: Hash,
      }).enc({ streamId }).buffer,
    );
    const resp = Result(StreamInfo, str).dec(data);
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async updateModule(streamId: string, moduleId: string): Promise<void> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/update_module",
      Struct({
        streamId: Hash,
        moduleId: Hash,
      }).enc({ streamId, moduleId }).buffer,
    );
    const resp = Result(_void, str).dec(data);
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
      StreamEventBatchArgs.enc({ streamId, events }).buffer,
    );
    const resp = Result(_void, str).dec(data);
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
      StreamSubscribeArgs.enc({ streamId, query }).buffer,
    );
    const resp = Result(Ulid, str).dec(data);
    if (!resp.success) {
      throw new Error(resp.value);
    }

    const subId = resp.value;
    this.#querySubscriptions.set(subId, handler);
    return async () => {
      const data: Uint8Array = await this.socket.emitWithAck(
        "stream/unsubscribe",
        Ulid.enc(subId).buffer,
      );
      this.#querySubscriptions.delete(subId);
      const resp = Result(bool, str).dec(data);
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
      StreamQueryArgs.enc({ streamId, query }).buffer,
    );
    const resp = StreamQueryResp.dec(data);
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }
}
