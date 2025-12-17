import { BytesWrapper, CidLink, CidLinkWrapper } from "@atcute/cbor";

export type Did = string & { __brand: "did" };
export type SubscriptionId = string & { __brand: "subscription_id" };

export type ModuleCodec<ModuleType extends string = string, Def = unknown> = {
  type: ModuleType;
  def: Def;
};

export type BasicModule = ModuleCodec<
  "space.roomy.module.basic.0",
  {
    init_sql: string;
    authorizer: string;
    materializer: string;
    queries: {
      name: string;
      sql: string;
      params: {}[];
    }[];
  }
>;

export type SqlValue =
  | "Null"
  | { Integer: number }
  | { Real: number }
  | { Text: string }
  | { Blob: BytesWrapper };

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
export type ModuleUploadResp = Result<{ cid: CidLinkWrapper }>;

export type ModuleExistsArgs = {
  cid: CidLink;
};
export type ModuleExistsResp = Result<{ module_exists: boolean }>;

export type StreamCreateArgs = {
  module_cid: CidLink;
};

export type StreamCreateResp = Result<{ stream_did: Did }>;

export type StreamInfoArgs = {
  stream_did: Did;
};
export type StreamInfoResp = Result<{
  module_cid: CidLinkWrapper;
}>;

export type StreamUpdateModuleArgs = {
  stream_did: Did;
  module_cid: CidLink;
};
export type StreamUpdateModuleResp = Result<void>;

export type StreamEventBatchArgs = {
  stream_did: Did;
  events: EventPayload[];
};
export type StreamEventBatchResp = Result<void>;

export type StreamSubscribeArgs = {
  stream_did: Did;
  query: LeafQuery;
};
export type StreamSubscribeNotification = {
  subscription_id: SubscriptionId;
  response: Result<SqlRows>;
};
export type StreamSubscribeResp = Result<{ subscription_id: SubscriptionId }>;

export type StreamUnsubscribeArgs = { subscription_id: SubscriptionId };
export type StreamUnsubscribeResp = Result<{ was_subscribed: boolean }>;

export type StreamQueryArgs = {
  stream_did: Did;
  query: LeafQuery;
};

export type StreamQueryResp = Result<SqlRows>;
