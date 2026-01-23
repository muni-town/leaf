import { io, Socket } from "socket.io-client";
import parser from "socket.io-msgpack-parser";
import { encode, decode, CidLinkWrapper, BytesWrapper } from "@atcute/cbor";
import { Cid, create as createCid } from "@atcute/cid";
import {
  AssertOk,
  BasicModule,
  Did,
  LeafQuery,
  ModuleCodec,
  ModuleExistsArgs,
  ModuleExistsResp,
  ModuleUploadArgs,
  ModuleUploadResp,
  Result,
  SqlRows,
  StreamCreateArgs,
  StreamCreateResp,
  StreamEventBatchArgs,
  StreamEventBatchResp,
  StreamInfoArgs,
  StreamInfoResp,
  StreamQueryArgs,
  StreamQueryResp,
  StreamSetHandleArgs,
  StreamSetHandleResp,
  StreamSubscribeArgs,
  StreamSubscribeNotification,
  StreamSubscribeResp,
  StreamUnsubscribeArgs,
  StreamUnsubscribeResp,
  StreamUpdateModuleArgs,
  StreamUpdateModuleResp,
  SubscribeEventsResp,
  SubscriptionId,
} from "./codec.js";

export * from "./codec.js";

type SocketIoBuffer = Buffer | ArrayBuffer;

async function createDaslCid(bytes: Uint8Array): Promise<Cid> {
  return createCid(0x71, bytes);
}

// Helper to ensure binary data is in the correct format for socket.io-msgpack-parser
// In Node.js, the parser requires Buffer. In browsers, ArrayBuffer is necessary.
function toBinary(data: Uint8Array): SocketIoBuffer {
  // Check if Buffer is available (Node.js environment)
  if (typeof Buffer !== "undefined" && Buffer.from) {
    return Buffer.from(data);
  } else {
    return data.buffer.slice(0, data.length) as ArrayBuffer;
  }
}

// Helper to convert response data back to plain Uint8Array for decoding Buffer in Node.js can cause
// issues with decoding
function fromBinary(data: Uint8Array | SocketIoBuffer): Uint8Array {
  if (data instanceof Uint8Array) return data;
  return new Uint8Array(data);
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
    (result: Result<SubscribeEventsResp>) => void | Promise<void>
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
        sub(convertBytesWrappers(notification.response));
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

  async uploadBasicModule(module: BasicModule): Promise<{ moduleCid: string }> {
    return this.uploadModule(module);
  }

  static async moduleCid(module: BasicModule): Promise<string> {
    const encoded = encode(module);
    const moduleCid = await createDaslCid(encoded);
    return new CidLinkWrapper(moduleCid.bytes).$link;
  }

  async uploadModule(module: ModuleCodec): Promise<{ moduleCid: string }> {
    const req = toBinary(encode({ module } satisfies ModuleUploadArgs));
    const data: SocketIoBuffer = await this.socket.emitWithAck(
      "module/upload",
      req,
    );
    const resp: ModuleUploadResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return { moduleCid: resp.Ok.moduleCid.$link };
  }

  async hasModule(moduleCid: string): Promise<boolean> {
    console.log(moduleCid);
    const data: SocketIoBuffer = await this.socket.emitWithAck(
      "module/exists",
      toBinary(
        encode({ moduleCid: { $link: moduleCid } } satisfies ModuleExistsArgs),
      ),
    );
    const resp: ModuleExistsResp = decode(fromBinary(data));
    console.log(resp);
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok.moduleExists;
  }

  async createStream(moduleCid: string): Promise<{ streamDid: string }> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/create",
      toBinary(
        encode({ moduleCid: { $link: moduleCid } } satisfies StreamCreateArgs),
      ),
    );
    const resp: StreamCreateResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
    return resp.Ok;
  }

  async streamInfo(streamDid: string): Promise<{ moduleCid?: string }> {
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
    return { moduleCid: resp.Ok.moduleCid?.$link };
  }

  async updateModule(
    streamDid: string,
    moduleCid: string,
  ): Promise<AssertOk<StreamUpdateModuleResp>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/update_module",
      toBinary(
        encode({
          moduleCid: { $link: moduleCid },
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

  async sendEvent(streamDid: string, event: Uint8Array): Promise<void> {
    await this.sendEvents(streamDid as Did, [event]);
  }

  async sendEvents(streamDid: string, events: Uint8Array[]): Promise<void> {
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
  async subscribeEvents(
    streamDid: string,
    query: LeafQuery,
    handler: (resp: Result<SubscribeEventsResp>) => Promise<void> | void,
  ): Promise<() => Promise<void>> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/subscribe_events",
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

  async query(streamDid: string, query: LeafQuery): Promise<SqlRows> {
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
    return convertBytesWrappers(resp.Ok);
  }

  async setHandle(streamDid: string, handle: string | null): Promise<void> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/set_handle",
      toBinary(
        encode({
          streamDid: streamDid as Did,
          handle,
        } satisfies StreamSetHandleArgs),
      ),
    );
    const resp: StreamSetHandleResp = decode(fromBinary(data));
    if ("Err" in resp) {
      throw new Error(resp.Err);
    }
  }
}

function convertBytesWrappers(t: any): any {
  if (t instanceof BytesWrapper) {
    return t.buf;
  } else if (typeof t == "object") {
    for (const key in t) {
      t[key] = convertBytesWrappers(t[key]);
    }
    return t;
  } else {
    return t;
  }
}
