import { denoKvToolboxStorageAdapter } from "../src/storage/deno-kv-toolbox.ts";
import { denoKvStorageAdapter } from "../src/storage/deno-kv.ts";

const denoKvPath = Deno.args[0];
const denoKvToolboxPath = Deno.args[1];
if (!denoKvPath || !denoKvToolboxPath)
  throw "Usage: deno run --unsable-kv migrateDenoKvToDenoKvToolbox <denoKvPath> <denoKvToolboxPath>";

const fromAdapter = denoKvStorageAdapter(await Deno.openKv(denoKvPath));
const toAdapter = denoKvToolboxStorageAdapter(
  await Deno.openKv(denoKvToolboxPath)
);

const allData = await fromAdapter.loadRange([]);

for (const record of allData) {
  if (record.data) await toAdapter.save(record.key, record.data);
}
