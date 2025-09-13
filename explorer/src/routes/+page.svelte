<script lang="ts">
	import { backend } from '$lib/workers';
	import type { IncomingEvent } from '@muni-town/leaf-client';
	import { getContext } from 'svelte';

	let loading = $state(false);

	const events = getContext<(IncomingEvent | string)[]>('events');
	const streamId = getContext<{ value: string }>('streamId');

	let offset = $state(1);
	let limit = $state(100);

	let moduleId = $state(localStorage.getItem('module') || '');
	$effect(() => {
		localStorage.setItem('module', moduleId);
	});

	let moduleFileInput = $state(undefined) as undefined | FileList;

	let payload = $state('');
</script>

<!-- Fetch Events -->
<form
	class="m-8 flex flex-col gap-2"
	onsubmit={async () => {
		loading = true;
		try {
			const evs = await backend.fetchEvents(streamId.value, offset, limit);
			events.push(`Finished fetch: ${evs.length} items`);
			evs.forEach((e) =>
				events.push(
					`${e.idx}(${e.user}) - ${e.stream}:\n    ${new TextDecoder().decode(e.payload)}`
				)
			);
		} catch (e: any) {
			events.push(e.toString());
		}
		loading = false;
	}}
>
	<h2 class="mb-4 text-xl font-bold">Fetch Events</h2>
	Offset
	<input class="input w-full" type="number" bind:value={offset} />
	Limit
	<input class="input w-full" type="number" bind:value={limit} />
	<button class="btn btn-outline" disabled={loading}>Fetch</button>
</form>

<!-- Send Event -->
<form
	class="m-8 flex flex-col gap-2"
	onsubmit={async () => {
		loading = true;
		try {
			await backend.sendEvent(streamId.value, new TextEncoder().encode(payload).buffer);
			events.push(`Sent event`);
		} catch (e: any) {
			events.push(e.toString());
		}
		loading = false;
	}}
>
	<h2 class="mb-4 text-xl font-bold">Send Event</h2>
	Payload
	<textarea class="input h-20 w-full" bind:value={payload}></textarea>
	<button class="btn btn-outline" disabled={loading}>Send</button>
</form>

<!-- Create Stream -->
<form
	class="m-8 flex flex-col gap-2"
	onsubmit={async () => {
		loading = true;
		try {
			const id = await backend.createStream(moduleId, new ArrayBuffer());
			streamId.value = id;
			events.push(`Created stream: ${id}`);
		} catch (e: any) {
			events.push(e.toString());
		}
		loading = false;
	}}
>
	<h2 class="mb-4 text-xl font-bold">Create Stream</h2>
	Module
	<input class="input w-full" bind:value={moduleId} />
	<button class="btn btn-outline" disabled={loading}>Create</button>
</form>

<!-- Upload Module -->
<form
	class="m-8 flex flex-col gap-2"
	onsubmit={async () => {
		loading = true;
		try {
			const file = moduleFileInput?.item(0);
			if (!file) throw 'Select file';
			const buffer = await file.arrayBuffer();
			const id = await backend.uploadModule(buffer);
			moduleId = id;
			events.push(`uploaded module: ${moduleId}`);
		} catch (e: any) {
			events.push(e.toString());
		}
		loading = false;
	}}
>
	<h2 class="mb-4 text-xl font-bold">Create Module</h2>
	Module
	<input class="file-input" bind:files={moduleFileInput} type="file" accept=".wasm" />
	<button class="btn btn-outline" disabled={loading}>Upload</button>
</form>

<!-- Has Module -->
<form
	class="m-8 flex flex-col gap-2"
	onsubmit={async () => {
		loading = true;
		try {
			const hasModule = await backend.hasModule(moduleId);
			events.push(hasModule ? `Has module: ${moduleId}` : `No module: ${moduleId}`);
		} catch (e: any) {
			events.push(e.toString());
		}
		loading = false;
	}}
>
	<h2 class="mb-4 text-xl font-bold">Has Module</h2>
	Module
	<input class="input w-full" bind:value={moduleId} />
	<button class="btn btn-outline" disabled={loading}>Check</button>
</form>
