import { io, Socket } from "socket.io-client";
import parser from "socket.io-msgpack-parser";
import {
  encode,
  decode,
  CidLinkWrapper,
  CidLink,
  BytesWrapper,
} from "@atcute/cbor";
import { Cid, create as createCid } from "@atcute/cid";
import {
  AssertOk,
  BasicModule,
  Did,
  EventPayload,
  LeafQuery,
  ModuleCodec,
  ModuleExistsArgs,
  ModuleExistsResp,
  ModuleUploadArgs,
  ModuleUploadResp,
  StreamCreateArgs,
  StreamCreateResp,
  StreamEventBatchArgs,
  StreamEventBatchResp,
  StreamInfoArgs,
  StreamInfoResp,
  StreamQueryArgs,
  StreamQueryResp,
  StreamSubscribeArgs,
  StreamSubscribeNotification,
  StreamSubscribeResp,
  StreamUnsubscribeArgs,
  StreamUnsubscribeResp,
  StreamUpdateModuleArgs,
  StreamUpdateModuleResp,
  SubscriptionId,
} from "./codec.js";

export * from "./codec.js";

async function createDaslCid(bytes: Uint8Array): Promise<Cid> {
  return createCid(0x71, bytes);
}

// Helper to ensure binary data is in the correct format for socket.io-msgpack-parser
// In Node.js, the parser requires Buffer. In browsers, Uint8Array works.
function toBinary(data: Uint8Array): Uint8Array {
  if (typeof Buffer !== "undefined" && Buffer.from) {
    return Buffer.from(data);
  }
  return data;
}

// Helper to convert response data back to plain Uint8Array for scale-ts decoding
// Buffer in Node.js can cause issues with scale-ts decoding
function fromBinary(data: Uint8Array): Uint8Array {
  // If it's a Buffer, convert to plain Uint8Array
  if (
    (typeof Buffer !== "undefined" && Buffer.isBuffer(data)) ||
    data instanceof ArrayBuffer
  ) {
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
    SubscriptionId,
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
      const notification: StreamSubscribeNotification = decode(
        fromBinary(data),
      );
      const sub = this.#querySubscriptions.get(notification.subscriptionId);
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

  async uploadBasicModule(
    module: BasicModule,
  ): Promise<AssertOk<ModuleUploadResp>> {
    return this.uploadModule(module);
  }

  static async encodeModule(module: BasicModule): Promise<{
    encoded: Uint8Array;
    moduleCid: CidLinkWrapper;
  }> {
    const encoded = encode(module);
    const moduleCid = await createDaslCid(encoded);
    return { encoded, moduleCid: new CidLinkWrapper(moduleCid.bytes) };
  }

  async uploadModule(module: ModuleCodec): Promise<AssertOk<ModuleUploadResp>> {
    const req = toBinary(encode({ module } satisfies ModuleUploadArgs));
    const data: Uint8Array = await this.socket.emitWithAck(
      "module/upload",
      req,
    );
    const resp: ModuleUploadResp = decode(fromBinary(data));
    console.log(resp);
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok;
  }

  async hasModule(moduleCid: CidLink): Promise<AssertOk<ModuleExistsResp>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "module/exists",
      toBinary(encode({ moduleCid: moduleCid } satisfies ModuleExistsArgs)),
    );
    const resp: ModuleExistsResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok;
  }

  async createStream(moduleCid: CidLink): Promise<AssertOk<StreamCreateResp>> {
    console.log(moduleCid);
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/create",
      toBinary(encode({ moduleCid } satisfies StreamCreateArgs)),
    );
    const resp: StreamCreateResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok;
  }

  async streamInfo(streamDid: string): Promise<AssertOk<StreamInfoResp>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/info",
      toBinary(
        encode({ streamDid: streamDid as Did } satisfies StreamInfoArgs),
      ),
    );
    const resp: StreamInfoResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok;
  }

  async updateModule(
    streamDid: string,
    moduleCid: CidLink,
  ): Promise<AssertOk<StreamUpdateModuleResp>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/update_module",
      toBinary(
        encode({
          moduleCid,
          streamDid: streamDid as Did,
        } satisfies StreamUpdateModuleArgs),
      ),
    );
    const resp: StreamUpdateModuleResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok;
  }

  async sendEvent(
    streamDid: string,
    event: Uint8Array,
  ): Promise<AssertOk<StreamEventBatchResp>> {
    this.sendEvents(streamDid as Did, [event]);
  }

  async sendEvents(
    streamDid: string,
    events: Uint8Array[],
  ): Promise<AssertOk<StreamEventBatchResp>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/event_batch",
      toBinary(
        encode({
          streamDid: streamDid as Did,
          events: events.map((x) => new BytesWrapper(x)),
        } satisfies StreamEventBatchArgs),
      ),
    );
    const resp: StreamEventBatchResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
  }

  /** Returns a function that can be called to unsubscribe the query. */
  async subscribe(
    streamDid: string,
    query: LeafQuery,
    handler: (resp: StreamQueryResp) => Promise<void> | void,
  ): Promise<() => Promise<void>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/subscribe",
      toBinary(
        encode({
          streamDid: streamDid as Did,
          query,
        } satisfies StreamSubscribeArgs),
      ),
    );
    const resp: StreamSubscribeResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }

    const subId = resp.Ok;
    this.#querySubscriptions.set(subId.subscriptionId, handler);
    return async () => {
      const data: Uint8Array = await this.socket.emitWithAck(
        "stream/unsubscribe",
        toBinary(encode(subId satisfies StreamUnsubscribeArgs)),
      );
      this.#querySubscriptions.delete(subId.subscriptionId);
      const resp: StreamUnsubscribeResp = decode(fromBinary(data));
      if ("Err" in resp) {
        throw new Error(`Error unsubscribing to query: ${resp.Err}`);
      }
    };
  }

  disconnect() {
    this.socket.disconnect();
    for (const key in this._listeners) {
      this._listeners[key as keyof typeof this._listeners] = [];
    }
  }

  async query(
    streamDid: string,
    query: LeafQuery,
  ): Promise<AssertOk<StreamQueryResp>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/query",
      toBinary(
        encode({
          streamDid: streamDid as Did,
          query,
        } satisfies StreamQueryArgs),
      ),
    );
    const resp: StreamQueryResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok;
  }
}
