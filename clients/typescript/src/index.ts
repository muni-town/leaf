import { io, Socket } from "socket.io-client";
import parser from "socket.io-msgpack-parser";

export type IncomingEvent = {
  stream: string;
} & StreamEvent;

export type StreamEvent = {
  idx: number;
  user: string;
  payload: ArrayBuffer;
};

export type StreamGenesis = {
  /** ULID that acts as a timestamp and unique identifier. */
  stamp: string;
  /** User account that created the stream. */
  creator: string;
  /**
   * Hex-encoded module ID that the stream was created with.
   *
   * > **Note:** If there has been a module update since the stream was created this will not be the
   * > current module.
   */
  module: string;
  /** The parameters used to initialize the module when the stream was created. */
  params: ArrayBuffer;
};

type EventMap = {
  connect: () => void;
  disconnect: () => void;
  authenticated: (did: string) => void;
  event: (event: IncomingEvent) => void;
  error: (error: string) => void;
};

export class LeafClient {
  socket: Socket;
  _listeners: { [K in keyof EventMap]: EventMap[K][] } = {
    connect: [],
    disconnect: [],
    authenticated: [],
    event: [],
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
    this.socket.on(
      "stream/event",
      (event: {
        stream: string;
        user: string;
        idx: number;
        payload: ArrayBuffer;
      }) => {
        this.#emit("event", event);
      },
    );
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

  async uploadModule(data: ArrayBuffer): Promise<string> {
    const resp: { hash: string } | { error: string } =
      await this.socket.emitWithAck("wasm/upload", data);
    if ("error" in resp) {
      throw new Error(resp.error);
    }
    return resp.hash;
  }

  async hasModule(wasmId: string): Promise<boolean> {
    const resp: { hasModule: boolean } | { error: string } =
      await this.socket.emitWithAck("wasm/has", wasmId);
    if ("error" in resp) {
      throw new Error(resp.error);
    }
    return resp.hasModule;
  }

  async createStream(
    ulid: string,
    wasmId: string,
    params: ArrayBuffer,
  ): Promise<string> {
    const resp: { streamId: string } | { error: string } =
      await this.socket.emitWithAck("stream/create", {
        ulid,
        module: wasmId,
        params,
      });
    if ("error" in resp) {
      throw new Error(resp.error);
    }
    return resp.streamId;
  }

  /** Helper to create a stream from a WASM module at a given URL, avoiding uploading / downloading
   * the WASM if the module ID already exists on the server. */
  async createStreamFromModuleUrl(
    ulid: string,
    moduleId: string,
    url: string,
    params: ArrayBuffer,
  ): Promise<string> {
    const hasModule = await this.hasModule(moduleId);

    if (!hasModule) {
      const resp = await fetch(url);
      const data = await resp.blob();
      const buffer = await data.arrayBuffer();
      const uploadedId = await this.uploadModule(buffer);
      if (uploadedId !== moduleId)
        throw new Error(
          `The module ID that was uploaded didn't match the expected module ID. Expected ${moduleId} got ${uploadedId}`,
        );
    }

    return await this.createStream(ulid, moduleId, params);
  }

  async sendEvent(streamId: string, payload: ArrayBuffer): Promise<void> {
    const resp: { ok: true } | { error: string } =
      await this.socket.emitWithAck("stream/event", {
        id: streamId,
        payload,
      });
    if ("error" in resp) {
      throw new Error(resp.error);
    }
  }

  async sendEvents(streamId: string, payloads: ArrayBuffer[]): Promise<void> {
    const resp: { ok: true } | { error: string } =
      await this.socket.emitWithAck("stream/event_batch", {
        id: streamId,
        payloads,
      });
    if ("error" in resp) {
      throw new Error(resp.error);
    }
  }

  async fetchEvents(
    streamId: string,
    opts?: {
      offset?: number;
      limit?: number;
      filter?: ArrayBuffer;
    },
  ): Promise<{ idx: number; user: string; payload: ArrayBuffer }[]> {
    opts = { ...{ offset: 1, limit: 100 }, ...(opts || {}) };
    // Remove any "undefined" values which get serialized and don't need to be sent over the
    // network.
    for (const key of Object.keys(opts) as (keyof typeof opts)[]) {
      if (!opts[key]) {
        delete opts[key];
      }
    }
    const resp:
      | { events: { idx: number; user: string; payload: ArrayBuffer }[] }
      | { error: string } = await this.socket.emitWithAck("stream/fetch", {
      id: streamId,
      ...opts,
    });
    if ("error" in resp) {
      throw new Error(resp.error);
    }
    return resp.events;
  }

  async streamInfo(streamId: string): Promise<StreamGenesis> {
    const resp: StreamGenesis | { error: string } =
      await this.socket.emitWithAck("stream/info", {
        stream_id: streamId,
      });
    if ("error" in resp) {
      throw new Error(resp.error);
    }
    return resp;
  }

  async subscribe(streamId: string): Promise<void> {
    const resp: { ok: true } | { error: string } =
      await this.socket.emitWithAck("stream/subscribe", streamId);
    if ("error" in resp) {
      throw new Error(resp.error);
    }
  }

  /**
   * @returns whether or not the connection was previously subscribed.
   */
  async unsubscribe(streamId: string): Promise<boolean> {
    const resp: { ok: true; wasUnSubscribed: boolean } | { error: string } =
      await this.socket.emitWithAck("stream/unsubscribe", streamId);
    if ("error" in resp) {
      throw new Error(resp.error);
    }
    return resp.wasUnSubscribed;
  }

  disconnect() {
    this.socket.disconnect();
    for (const key in this._listeners) {
      this._listeners[key as keyof typeof this._listeners] = [];
    }
  }
}
