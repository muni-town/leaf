import {
  denoKvBlobStorageAdapter,
  denoKvSmallStorageAdapter,
} from "@muni-town/leaf-storage-deno-kv";

const denoKvPath = Deno.args[0];
const denoKvToolboxPath = Deno.args[1];
if (!denoKvPath || !denoKvToolboxPath)
  throw "Usage: deno run --unsable-kv migrateDenoKvToDenoKvToolbox <denoKvPath> <denoKvToolboxPath>";

const fromAdapter = denoKvSmallStorageAdapter(await Deno.openKv(denoKvPath));
const toAdapter = denoKvBlobStorageAdapter(
  await Deno.openKv(denoKvToolboxPath)
);

const allData = await fromAdapter.loadRange([]);

for (const record of allData) {
  if (record.data) await toAdapter.save(record.key, record.data);
}
