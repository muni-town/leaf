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
    let view = new DataView(buffer);
    view.setFloat64(0, n, true);
    return new Uint8Array(buffer);
  },
  (b) => {
    let view = new DataView(b.buffer);
    return view.getFloat64(0, true);
  },
);

export const Hash = enhanceCodec(Bytes(32), hex.decode, hex.encode);
export const Ulid = enhanceCodec(Bytes(16), crockfordDecode, crockfordEncode);

export const LeafModuleCodec = Struct({
  moduleTypeId: str,
  data: Bytes(),
});
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
export type BasicModuleDef = CodecType<typeof BasicModuleDef>;
export const BasicModuleDef = Struct({
  init_sql: str,
  authorizer: str,
  materializer: str,
  queries: Vector(LeafModuleQueryDef),
});
export type StreamGenesis = CodecType<typeof StreamGenesis>;
export const StreamGenesis = Struct({
  stamp: Ulid,
  creator: str,
  module: Hash,
  options: Vector(Enum({})),
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

export type StreamInfo = CodecType<typeof StreamInfo>;
export const StreamInfo = Struct({
  creator: str,
  moduleId: Hash,
});

export type LeafQuery = CodecType<typeof LeafQuery>;
const LeafQuery = Struct({
  query_name: str,
  requesting_user: Option(str),
  params: Vector(Tuple(str, SqlValue)),
  start: Option(i64),
  limit: Option(i64),
});

export const StreamEventBatchArgs = Struct({
  streamId: Hash,
  events: Vector(IncomingEvent),
});

export type StreamQueryArgs = CodecType<typeof StreamQueryArgs>;
export const StreamQueryArgs = Struct({
  streamId: Hash,
  query: LeafQuery,
});

export type StreamQueryResp = CodecType<typeof StreamQueryResp>;
export const StreamQueryResp = Result(SqlRows, str);

export type StreamSubscribeNotification = CodecType<
  typeof StreamSubscribeNotification
>;
export const StreamSubscribeNotification = Struct({
  subscription_id: Ulid,
  response: StreamQueryResp,
});

export type StreamSubscribeArgs = CodecType<typeof StreamSubscribeArgs>;
export const StreamSubscribeArgs = Struct({
  streamId: Hash,
  query: LeafQuery,
});

export const HasModuleResp = Result(bool, str);

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
