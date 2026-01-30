import { BytesWrapper, Bytes, CidLink, CidLinkWrapper } from "@atcute/cbor";

export { CidLinkWrapper, BytesWrapper };
export type { CidLink, Bytes };
export type Did = string & { __brand: "did" };
export type SubscriptionId = string & { __brand: "subscriptionId" };

export type ModuleCodec<ModuleType extends string = string, Def = unknown> = {
  $type: ModuleType;
} & Def;

export type BasicModule = ModuleCodec<
  "muni.town.leaf.module.basic.v0",
  {
    initSql: string;
    authorizer: string;
    materializer: string;
    stateMaterializer: string;
    stateInitSql: string;
    queries: {
      name: string;
      sql: string;
      params: {
        name: string;
        kind: "integer" | "real" | "text" | "blob" | "any";
        optional: boolean;
      }[];
    }[];
  }
>;

export type SqlValueRaw =
  | { $type: "muni.town.sqliteValue.null" }
  | { $type: "muni.town.sqliteValue.integer"; value: number }
  | { $type: "muni.town.sqliteValue.real"; value: number }
  | { $type: "muni.town.sqliteValue.text"; value: string }
  | { $type: "muni.town.sqliteValue.blob"; value: BytesWrapper };

export type SqlValue =
  | { $type: "muni.town.sqliteValue.null" }
  | { $type: "muni.town.sqliteValue.integer"; value: number }
  | { $type: "muni.town.sqliteValue.real"; value: number }
  | { $type: "muni.town.sqliteValue.text"; value: string }
  | { $type: "muni.town.sqliteValue.blob"; value: Uint8Array };

export type LeafQuery = {
  name: string;
  params: { [key: string]: SqlValueRaw };
  start?: number;
  limit?: number;
};

export type SqlRow<V extends SqlValue | SqlValueRaw = SqlValue> = {
  [column_name: string]: V;
};
export type SqlRows<V extends SqlValue | SqlValueRaw = SqlValue> = SqlRow<V>[];

export type SubscribeEventsResp<V extends SqlValue | SqlValueRaw = SqlValue> = {
  rows: SqlRows<V>;
  has_more: boolean;
};

export type EventPayload = BytesWrapper;

export type Result<T extends void | Record<string, any>, E = string> =
  | { Ok: T }
  | { Err: E };

export type AssertOk<R> = R extends { Ok: infer T } ? T : never;

export type ModuleUploadArgs = {
  module: ModuleCodec;
};
export type ModuleUploadResp = Result<{ moduleCid: CidLinkWrapper }>;

export type ModuleExistsArgs = {
  moduleCid: CidLink;
};
export type ModuleExistsResp = Result<{ moduleExists: boolean }>;

export type StreamCreateArgs = {
  moduleCid: CidLink;
};

export type StreamCreateResp = Result<{ streamDid: Did }>;

export type StreamInfoArgs = {
  streamDid: Did;
};
export type StreamInfoResp = Result<{
  moduleCid?: CidLinkWrapper;
}>;

export type StreamUpdateModuleArgs = {
  streamDid: Did;
  moduleCid: CidLink;
};
export type StreamUpdateModuleResp = Result<void>;

export type StreamEventBatchArgs = {
  streamDid: Did;
  events: EventPayload[];
};
export type StreamEventBatchResp = Result<void>;

export type StreamSubscribeArgs = {
  streamDid: Did;
  query: LeafQuery;
};
export type StreamSubscribeNotification = {
  subscriptionId: SubscriptionId;
  response: Result<SubscribeEventsResp>;
};
export type StreamSubscribeResp = Result<{ subscriptionId: SubscriptionId }>;

export type StreamUnsubscribeArgs = { subscriptionId: SubscriptionId };
export type StreamUnsubscribeResp = Result<{ was_subscribed: boolean }>;

export type StreamQueryArgs = {
  streamDid: Did;
  query: LeafQuery;
};
export type StreamQueryResp = Result<SqlRows<SqlValueRaw>>;

export type StreamSetHandleArgs = {
  streamDid: Did;
  handle: string | null;
};
export type StreamSetHandleResp = Result<void>;

export type StreamStateEventBatchArgs = {
  streamDid: Did;
  events: EventPayload[];
};
export type StreamStateEventBatchResp = Result<void>;

export type StreamClearStateArgs = {
  streamDid: Did;
};
export type StreamClearStateResp = Result<void>;