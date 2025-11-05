<script lang="ts">
	import EventList from '$lib/components/EventList.svelte';
	import CodeMirror from 'svelte-codemirror-editor';
	import { sql as sqlLang } from '@codemirror/lang-sql';
	import { oneDark as oneDarkTheme } from '@codemirror/theme-one-dark';

	import { backend, backendStatus } from '$lib/workers';
	import { getContext } from 'svelte';
	import type { StreamGenesis } from '@muni-town/leaf-client';
	import { ulid } from 'ulidx';

	let loading = $state(false);

	const events = getContext<string[]>('events');
	const streamId = getContext<{ value: string }>('streamId');

	let offset = $state(1);
	let limit = $state(100);

	let moduleId = $state(localStorage.getItem('module') || '');
	$effect(() => {
		localStorage.setItem('module', moduleId);
	});

	let moduleFileInput = $state(undefined) as undefined | FileList;

	const tabs = ['Query', 'Create Stream'] as const;
	let currentTab = $state('Create Stream') as (typeof tabs)[number];

	let payload = $state('');

	const defaultGenesis: StreamGenesis = {
		stamp: '',
		creator: '',
		module: {
			init_sql: '',
			authorizer: '',
			materializer: '',
			queries: [],
			wasm_hash: undefined
		},
		strict_module_updates: false
	};
	let newStreamGenesis: StreamGenesis = $state(
		JSON.parse(localStorage.getItem('streamGenesis') || JSON.stringify(defaultGenesis))
	);
	$effect(() => {
		localStorage.setItem('streamGenesis', JSON.stringify(newStreamGenesis || defaultGenesis));
	});

	async function createStream() {
		if (!backendStatus.did) return;
		newStreamGenesis.stamp = ulid();
		newStreamGenesis.creator = backendStatus.did;
		streamId.value = await backend.createStream($state.snapshot(newStreamGenesis));
	}
</script>

<div class="flex min-h-0 min-w-0 shrink flex-row gap-3 px-5">
	<div class="border-accent bg-base-100 thin-scroll w-[24em] shrink overflow-y-auto shadow-md">
		<div role="tablist" class="tabs tabs-border">
			{#each tabs as tab}
				<button
					role="tab"
					class="tab"
					class:tab-active={currentTab == tab}
					onclick={() => (currentTab = tab)}>{tab}</button
				>
			{/each}
		</div>
		{#if currentTab == 'Query'}
			<!-- Fetch Events
			<form
				class="m-8 flex flex-col gap-2"
				onsubmit={async () => {
				}}
			>
				<h2 class="mb-4 text-xl font-bold">Fetch Events</h2>
				Offset
				<input class="input w-full" type="number" bind:value={offset} />
				Limit
				<input class="input w-full" type="number" bind:value={limit} />
				<button class="btn btn-outline" disabled={loading}>Fetch</button>
			</form> -->

			<!-- Send Event 
			<form
				class="m-8 flex flex-col gap-2"
				onsubmit={async () => {
					loading = true;
					try {
						await backend.sendEvent(
							streamId.value,
							new TextEncoder().encode(payload).buffer
						);
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
			</form> -->

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
						events.push(
							hasModule ? `Has module: ${moduleId}` : `No module: ${moduleId}`
						);
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
		{:else if currentTab == 'Create Stream'}
			<button class="btn m-3 w-40" onclick={createStream}>Create Stream</button>
		{/if}
	</div>

	<div class="bg-base-100 thin-scroll min-w-[20em] grow overflow-auto shadow-md">
		{#if currentTab == 'Query'}
			<EventList />
		{:else if currentTab == 'Create Stream'}
			<div>
				<h2 class="m-3 text-xl font-bold">Init SQL</h2>
				<p class="m-3 text-sm opacity-40">
					This code will be run to initialize the module database and should be
					idempotent.
				</p>
				<CodeMirror
					lang={sqlLang()}
					bind:value={newStreamGenesis.module.init_sql}
					lineNumbers={false}
					theme={oneDarkTheme}
					placeholder="CREATE TABLE IF NOT EXISTS example ();"
				/>
				<h2 class="m-3 text-xl font-bold">Authorizer SQL</h2>
				<div class="m-3 gap-2 text-sm opacity-40">
					<p>SQL used to authorize new events before the are accepted into the stream.</p>
					<p>
						To access the event that is being authorized you can query the <code
							>user</code
						>
						and <code>payload</code> from the <code>next_event</code> table.
					</p>
				</div>
				<CodeMirror
					lang={sqlLang()}
					bind:value={newStreamGenesis.module.authorizer}
					lineNumbers={false}
					theme={oneDarkTheme}
					placeholder="-- authorization SQL"
				/>
				<h2 class="m-3 text-xl font-bold">Materializer SQL</h2>
				<div class="m-3 gap-2 text-sm opacity-40">
					<p>SQL used to authorize new events before the are accepted into the stream.</p>
					<p>
						To access the event that is being materialized you can query the <code
							>user</code
						>
						and <code>payload</code> from the <code>next_event</code> table.
					</p>
				</div>
				<CodeMirror
					lang={sqlLang()}
					bind:value={newStreamGenesis.module.materializer}
					lineNumbers={false}
					theme={oneDarkTheme}
					placeholder="-- materialization sql"
				/>
				<h2 class="mx-3 mb-2 mt-5 flex items-center text-xl font-bold">
					Queries
					<div class="grow"></div>
					<button
						class="btn"
						onclick={() => {
							newStreamGenesis.module.queries.push({
								name: '',
								sql: '',
								limits: [],
								params: []
							});
						}}>+</button
					>
				</h2>
				<hr class="my-3" />
				{#each newStreamGenesis.module.queries as query, i}
					<div class="m-8 flex flex-col gap-3">
						<div class="flex gap-3">
							<input class="input" placeholder="query name" bind:value={query.name} />
							<button
								class="btn"
								onclick={() =>
									(newStreamGenesis.module.queries =
										newStreamGenesis.module.queries.splice(i, 0))}
								>Delete Query</button
							>
						</div>
						<CodeMirror
							lang={sqlLang()}
							bind:value={query.sql}
							lineNumbers={false}
							theme={oneDarkTheme}
							placeholder="-- query sql"
						/>
						<h3 class="flex items-center text-lg font-bold">
							Params
							<div class="grow"></div>
							<button
								class="btn"
								onclick={() => {
									query.params.push({
										optional: true,
										kind: {
											tag: 'any',
											value: undefined
										},
										name: ''
									});
								}}>+</button
							>
						</h3>
						{#each query.params as param, i}
							<div class="flex justify-between">
								<input
									class="input"
									placeholder="$myParam"
									bind:value={param.name}
								/>
								<select class="select" bind:value={param.kind}>
									<option selected value={{ tag: 'any', value: undefined }}
										>Any</option
									>
									<option value={{ tag: 'integer', value: undefined }}
										>Integer</option
									>
									<option value={{ tag: 'real', value: undefined }}>Real</option>
									<option value={{ tag: 'text', value: undefined }}>Text</option>
									<option value={{ tag: 'blob', value: undefined }}>Blob</option>
								</select>
								<label>
									Optional
									<input
										class="checkbox"
										type="checkbox"
										placeholder="Optional"
										bind:checked={param.optional}
									/>
								</label>
								<button
									class="btn"
									onclick={() => (query.params = query.params.splice(i, 0))}
									>Delete Param</button
								>
							</div>
						{/each}
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
