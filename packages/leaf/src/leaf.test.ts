import { assertEquals } from "jsr:@std/assert@1";

import { EntityId } from "./index.ts";

Deno.test("entity ID encode-decode round trip", () => {
  const e = new EntityId();
  const e2 = new EntityId(e.toString());

  assertEquals(e, e2);
  assertEquals(e.bytes, e2.bytes);
});
