import { BytesWrapper, Bytes, CidLink, CidLinkWrapper } from "@atcute/cbor";

export { CidLinkWrapper, BytesWrapper };
export type { CidLink, Bytes };
export type Did = string & { __brand: "did" };
export type SubscriptionId = string & { __brand: "subscriptionId" };

export type ModuleCodec<ModuleType extends string = string, Def = unknown> = {
  $type: ModuleType;
  def: Def;
};

export type BasicModule = ModuleCodec<
  "muni.town.leaf.module.basic.v0",
  {
    init_sql: string;
    authorizer: string;
    materializer: string;
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

export type SqlValue =
  | { $type: "muni.town.sqliteValue.null" }
  | { $type: "muni.town.sqliteValue.integer"; value: number }
  | { $type: "muni.town.sqliteValue.real"; value: number }
  | { $type: "muni.town.sqliteValue.text"; value: string }
  | { $type: "muni.town.sqliteValue.blob"; value: BytesWrapper };

export type LeafQuery = {
  name: string;
  user?: string;
  params: [string, SqlValue][];
  start?: number;
  limit?: number;
};

export type SqlRow = SqlValue[];
export type SqlRows = {
  rows: SqlRow[];
  column_names: string[];
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
export type ModuleExistsResp = Result<{ module_exists: boolean }>;

export type StreamCreateArgs = {
  moduleCid: CidLink;
};

export type StreamCreateResp = Result<{ streamDid: Did }>;

export type StreamInfoArgs = {
  streamDid: Did;
};
export type StreamInfoResp = Result<{
  moduleCid: CidLinkWrapper;
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
  response: Result<SqlRows>;
};
export type StreamSubscribeResp = Result<{ subscriptionId: SubscriptionId }>;

export type StreamUnsubscribeArgs = { subscriptionId: SubscriptionId };
export type StreamUnsubscribeResp = Result<{ was_subscribed: boolean }>;

export type StreamQueryArgs = {
  streamDid: Did;
  query: LeafQuery;
};

export type StreamQueryResp = Result<SqlRows>;
