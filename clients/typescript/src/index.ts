import { io, Socket } from "socket.io-client";
import parser from "socket.io-msgpack-parser";
import { hex } from "@scure/base";
import {
  _void,
  bool,
  Bytes,
  CodecType,
  enhanceCodec,
  Enum,
  i64,
  Option,
  Result,
  str,
  Struct,
  Tuple,
  Vector,
} from "scale-ts";

const f64 = enhanceCodec(
  Bytes(4),
  (n: number) => {
    let buffer = new ArrayBuffer(4);
    console.log(buffer);
    let view = new DataView(buffer);
    view.setFloat64(0, n, true);
    return new Uint8Array(buffer);
  },
  (b) => {
    console.log(b.buffer);
    let view = new DataView(b.buffer);
    return view.getFloat64(0, true);
  },
);

export const Hash = enhanceCodec(Bytes(32), hex.decode, hex.encode);
export const Ulid = enhanceCodec(Bytes(16), crockfordDecode, crockfordEncode);

const LeafModuleQueryParamKind = Enum({
  integer: _void,
  real: _void,
  text: _void,
  blob: _void,
  any: _void,
});
const LeafModuleQueryParamDef = Struct({
  name: str,
  kind: LeafModuleQueryParamKind,
  optional: bool,
});
const LeafModuleQueryDef = Struct({
  name: str,
  sql: str,
  params: Vector(LeafModuleQueryParamDef),
  limits: Vector(_void),
});
export type LeafModuleDef = CodecType<typeof LeafModuleDef>;
const LeafModuleDef = Struct({
  init_sql: str,
  authorizer: str,
  materializer: str,
  queries: Vector(LeafModuleQueryDef),
  wasm_hash: Option(Hash),
});
export type StreamGenesis = CodecType<typeof StreamGenesis>;
const StreamGenesis = Struct({
  stamp: Ulid,
  creator: str,
  module: LeafModuleDef,
  strict_module_updates: bool,
});

const SqlValue = Enum({
  null: _void,
  integer: i64,
  real: f64,
  text: str,
  blob: Bytes(),
});
const SqlRow = Struct({
  values: Vector(SqlValue),
});
export type SqlRows = CodecType<typeof SqlRows>;
const SqlRows = Struct({
  rows: Vector(SqlRow),
  column_names: Vector(str),
});

export type IncomingEvent = CodecType<typeof IncomingEvent>;
const IncomingEvent = Struct({
  user: str,
  payload: Bytes(),
});

export type LeafQuery = CodecType<typeof LeafQuery>;
const LeafQuery = Struct({
  query_name: str,
  requesting_user: str,
  params: Vector(Tuple(str, SqlValue)),
  start: Option(i64),
  end: Option(i64),
  limit: Option(i64),
});

const StreamEventBatchArgs = Struct({
  streamId: Hash,
  events: Vector(IncomingEvent),
});

export type StreamQueryArgs = CodecType<typeof StreamQueryArgs>;
const StreamQueryArgs = Struct({
  streamId: Hash,
  query: LeafQuery,
});

export type StreamQueryResp = CodecType<typeof StreamQueryResp>;
const StreamQueryResp = Result(SqlRows, str);

export type StreamSubscribeNotification = CodecType<
  typeof StreamSubscribeNotification
>;
const StreamSubscribeNotification = Struct({
  subscription_id: Ulid,
  response: StreamQueryResp,
});

export type LeafSubscribeQuery = CodecType<typeof LeafSubscribeQuery>;
const LeafSubscribeQuery = Struct({
  query_name: str,
  requesting_user: str,
  params: Vector(Tuple(str, SqlValue)),
  start: Option(i64),
  batch_size: Option(i64),
});
export type StreamSubscribeArgs = CodecType<typeof StreamSubscribeArgs>;
const StreamSubscribeArgs = Struct({
  streamId: Hash,
  query: LeafSubscribeQuery,
});

const HasWasmResp = Result(bool, str);

type EventMap = {
  connect: () => void;
  disconnect: () => void;
  authenticated: (did: string) => void;
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
  constructor(url: string, authenticator: () => Promise<string>) {
    this.socket = io(url, {
      parser,
      async auth(cb) {
        const token = await authenticator();
        cb({ token });
      },
    });

    this.socket.compress(true);
    this.socket.on("connect", () => {
      this.#emit("connect");
    });
    this.socket.on("disconnect", () => this.#emit("disconnect"));
    this.socket.on("authenticated", (data: { did: string }) => {
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

  async uploadWasm(wasm_data: ArrayBuffer): Promise<string> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "wasm/upload",
      wasm_data,
    );
    const resp = Result(Hash, str).dec(data);
    if (!resp.success) {
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async hasWasm(wasmId: string): Promise<boolean> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "wasm/has",
      Hash.enc(wasmId).buffer,
    );
    const resp = HasWasmResp.dec(data);
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
    console.log(data);
    const resp = Result(Hash, str).dec(data);
    if (!resp.success) {
      console.error(resp);
      throw new Error(resp.value);
    }
    return resp.value;
  }

  async updateModule(
    streamId: string,
    moduleDef: LeafModuleDef,
  ): Promise<void> {
    const data: Uint8Array = await this.socket.emitWithAck(
      "stream/update_module",
      Struct({
        streamId: Hash,
        moduleDef: LeafModuleDef,
      }).enc({ streamId, moduleDef }).buffer,
    );
    console.log(data);
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
    if (!genesis.module.wasm_hash)
      throw "No WASM module specified in stream genesis";

    const hasModule = await this.hasWasm(genesis.module.wasm_hash);

    if (!hasModule) {
      const resp = await fetch(url);
      const data = await resp.blob();
      const buffer = await data.arrayBuffer();
      const uploadedId = await this.uploadWasm(buffer);
      if (uploadedId !== genesis.module.wasm_hash)
        throw new Error(
          `The module ID that was uploaded didn't match the expected module ID. Expected ${genesis.module.wasm_hash} got ${uploadedId}`,
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
    query: LeafSubscribeQuery,
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
        throw new Error(`Error unsubscribing stream query: ${resp.value}`);
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

// Code from https://github.com/perry-mitchell/ulidx/blob/5043c511406fb9b836ddf126583c80ffb90cbb73/source/crockford.ts
// We already use ulidx but encoding is not exposed so we copy the functions here.
const B32_CHARACTERS = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
export function crockfordEncode(input: Uint8Array): string {
  const output: number[] = [];
  let bitsRead = 0;
  let buffer = 0;
  const reversedInput = new Uint8Array(input.slice().reverse());
  for (const byte of reversedInput) {
    buffer |= byte << bitsRead;
    bitsRead += 8;

    while (bitsRead >= 5) {
      output.unshift(buffer & 0x1f);
      buffer >>>= 5;
      bitsRead -= 5;
    }
  }
  if (bitsRead > 0) {
    output.unshift(buffer & 0x1f);
  }
  return output.map((byte) => B32_CHARACTERS.charAt(byte)).join("");
}
export function crockfordDecode(input: string): Uint8Array {
  const sanitizedInput = input.toUpperCase().split("").reverse().join("");
  const output: number[] = [];
  let bitsRead = 0;
  let buffer = 0;
  for (const character of sanitizedInput) {
    const byte = B32_CHARACTERS.indexOf(character);
    if (byte === -1) {
      throw new Error(
        `Invalid base 32 character found in string: ${character}`,
      );
    }
    buffer |= byte << bitsRead;
    bitsRead += 5;
    while (bitsRead >= 8) {
      output.unshift(buffer & 0xff);
      buffer >>>= 8;
      bitsRead -= 8;
    }
  }
  if (bitsRead >= 5 || buffer > 0) {
    output.unshift(buffer & 0xff);
  }
  return new Uint8Array(output);
}
