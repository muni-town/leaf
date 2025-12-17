import { BytesWrapper, CidLink, CidLinkWrapper } from "@atcute/cbor";

export type Did = string;

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

export type Result<T, E = string> = { Ok: T } | { Err: E };

export type ModuleUploadArgs = {
  module: ModuleCodec;
};
export type ModuleUploadResp = Result<CidLinkWrapper>;

export type ModuleExistsArgs = {
  cid: CidLink;
};
export type ModuleExistsResp = Result<boolean>;

export type StreamCreateArgs = {
  module_cid: CidLink;
};
// TODO: brand this as a DID?
export type StreamCreateResp = Result<string>;

export type StreamInfoArgs = {
  stream_did: string;
};
export type StreamInfoResp = Result<{
  module_cid: CidLinkWrapper;
}>;

export type StreamUpdateModuleArgs = {
  stream_did: string;
  module_cid: CidLink;
};
export type StreamUpdateModuleResp = Result<void>;

export type StreamEventBatchArgs = {
  stream_did: Did;
  events: EventPayload[];
};
export type StreamEventBatchResp = Result<void>;

export type StreamSubscribeArgs = {
  stream_did: string;
  query: LeafQuery;
};
export type StreamSubscribeNotification = {
  subscription_id: string;
  response: Result<SqlRows>;
};
export type StreamSubscribeResp = Result<string>;

export type StreamUnsubscribeArgs = string;
export type StreamUnsubscribeResp = Result<boolean>;

export type StreamQueryArgs = {
  stream_did: string;
  query: LeafQuery;
};

export type StreamQueryresp = Result<SqlRows>;
