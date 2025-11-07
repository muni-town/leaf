<script lang="ts">
	import EventList from '$lib/components/EventList.svelte';
	import CodeMirror from 'svelte-codemirror-editor';
	import { sql as sqlLang } from '@codemirror/lang-sql';
	import { json as jsonLang } from '@codemirror/lang-json';
	import { oneDark as oneDarkTheme } from '@codemirror/theme-one-dark';

	import { backend, backendStatus } from '$lib/workers';
	import { getContext } from 'svelte';
	import type { LeafQuery, StreamGenesis } from '@muni-town/leaf-client';
	import { ulid } from 'ulidx';
	import { page } from '$app/state';

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
	let currentTab = $derived(page.params.tab || '' in tabs ? page.params.tab : 'Query');

	let payload = $state(localStorage.getItem('payload') || '');
	$effect(() => {
		localStorage.setItem('payload', payload);
	});

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

	const defaultQuery: LeafQuery = {
		query_name: '',
		requesting_user: '',
		params: [],
		start: undefined,
		limit: undefined
	};
	const storedQuery = localStorage.getItem('query');
	let subscriptionId = $state('');
	let query: LeafQuery = $state(
		JSON.parse(
			storedQuery && storedQuery !== 'undefined' ? storedQuery : JSON.stringify(defaultQuery)
		)
	);
	$effect(() => {
		localStorage.setItem('query', JSON.stringify(query || defaultQuery));
	});

	async function createStream() {
		if (!backendStatus.did) return;
		newStreamGenesis.stamp = ulid();
		newStreamGenesis.creator = backendStatus.did;
		streamId.value = await backend.createStream($state.snapshot(newStreamGenesis));
	}
	async function updateModule() {
		if (!backendStatus.did) return;
		await backend.updateModule(streamId.value, $state.snapshot(newStreamGenesis.module));
	}

	async function runQuery() {
		if (!backendStatus.did) return;
		const q: typeof query = $state.snapshot(query) as any;
		q.requesting_user = backendStatus.did;
		for (const [_name, param] of q.params) {
			// The forms don't assign the proper data types to non-text params, so we convert them here.
			if (param.tag == 'blob') {
				param.value = new TextEncoder().encode(param.value as any);
			} else if (param.tag == 'integer') {
				param.value = BigInt(param.value);
			} else if (param.tag == 'real') {
				param.value = parseFloat(param.value as any);
			}
		}
		const result = await backend.query(streamId.value, q);
		events.push(
			JSON.stringify(
				result,
				(_key, value) => (typeof value === 'bigint' ? value.toString() : value),
				'  '
			)
		);
	}

	async function subscribe() {
		if (!backendStatus.did) return;
		const q: typeof query = $state.snapshot(query) as any;
		q.requesting_user = backendStatus.did;
		for (const [_name, param] of q.params) {
			// The forms don't assign the proper data types to non-text params, so we convert them here.
			if (param.tag == 'blob') {
				param.value = new TextEncoder().encode(param.value as any);
			} else if (param.tag == 'integer') {
				param.value = BigInt(param.value);
			} else if (param.tag == 'real') {
				param.value = parseFloat(param.value as any);
			}
		}
		subscriptionId = await backend.subscribe(streamId.value, q);
	}

	async function unsubscribe() {
		if (!subscriptionId) throw 'no subscription';
		await backend.unsubscribe(subscriptionId);
		subscriptionId = '';
	}

	async function sendEvent() {
		if (!backendStatus.did) return;
		await backend.sendEvents(streamId.value, [
			{ user: backendStatus.did, payload: new TextEncoder().encode(payload) }
		]);
	}
</script>

<div class="flex min-h-0 min-w-0 shrink flex-row gap-3 px-5">
	<div class="border-accent bg-base-100 thin-scroll w-[24em] shrink overflow-y-auto shadow-md">
		<div role="tablist" class="tabs tabs-border">
			{#each tabs as tab}
				<a href={`#/${tab}`} role="tab" class="tab" class:tab-active={currentTab == tab}
					>{tab}</a
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

			<form class="m-8 flex flex-col gap-2">
				<h2 class="mb-4 text-xl font-bold">Query</h2>

				Name
				<input class="input" placeholder="query name" bind:value={query.query_name} />
				<div class="flex gap-2">
					<input
						class="input input-sm"
						placeholder="start"
						bind:value={() => query.start, (v) => (query.start = v || undefined)}
					/>
					<input
						class="input input-sm"
						placeholder="limit"
						bind:value={() => query.limit, (v) => (query.limit = v || undefined)}
					/>
				</div>

				<h3 class="flex items-center text-lg font-bold">
					Params
					<div class="grow"></div>
					<button
						class="btn"
						type="button"
						onclick={() => {
							query.params.push([
								'$param',
								{
									tag: 'text',
									value: ''
								}
							]);
						}}>+</button
					>
				</h3>

				{#each query.params as [_name, value], i}
					<div class="flex items-center gap-3">
						<input class="input" bind:value={query.params[i][0]} placeholder="name" />
						<select class="select" bind:value={query.params[i][1].tag}>
							<option selected value="">Any</option>
							<option value="integer">Integer</option>
							<option value="real">Real</option>
							<option value="text">Text</option>
							<option value="blob">Blob</option>
						</select>
						<input class="input" bind:value={value.value} />
						<button
							type="button"
							onclick={() => query.params.splice(i, 1)}
							class="btn btn-sm">X</button
						>
					</div>
				{/each}

				<button type="submit" class="btn btn-outline" onclick={runQuery}>Query</button>
				{#if subscriptionId}
					Subscribed: {subscriptionId}
					<button type="submit" class="btn btn-outline" onclick={unsubscribe}
						>Unsubscribe</button
					>
				{:else}
					<button type="submit" class="btn btn-outline" onclick={subscribe}
						>Subscribe</button
					>
				{/if}
			</form>

			<!-- Upload WASM -->
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
				<h2 class="mb-4 text-xl font-bold">Upload WASM</h2>
				<input class="file-input" bind:files={moduleFileInput} type="file" accept=".wasm" />
				<button class="btn btn-outline" disabled={loading}>Upload</button>
			</form>

			<!-- Has WASM -->
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
				<h2 class="mb-4 text-xl font-bold">Has WASM</h2>
				<input class="input w-full" bind:value={moduleId} placeholder="wasm hash" />
				<button class="btn btn-outline" disabled={loading}>Check</button>
			</form>
		{:else if currentTab == 'Create Stream'}
			<div class="m-3 flex flex-col gap-2">
				<button class="btn w-40" onclick={createStream}>Create Stream</button>
				<button class="btn w-40" onclick={updateModule}>Update Module</button>
			</div>
		{/if}
	</div>

	<div class="bg-base-100 thin-scroll min-w-[20em] grow overflow-auto shadow-md">
		{#if currentTab == 'Query'}
			<div class="flex h-full flex-col gap-3">
				<h2 class="text-md m-2 flex items-center justify-between font-bold">
					Payload <button class="btn btn-sm" onclick={sendEvent}>Send Event</button>
				</h2>
				<CodeMirror
					lang={jsonLang()}
					bind:value={payload}
					lineNumbers={false}
					theme={oneDarkTheme}
					placeholder={`{"hello": "world"}`}
				/>
				<EventList />
			</div>
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
						and <code>payload</code> from the <code>event</code> table.
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
					<p>
						SQL used to materialize new events after they have been accepted into the
						stream.
					</p>
					<p>
						To access the event that is being materialized you can query the <code
							>user</code
						>
						and <code>payload</code> from the <code>event</code> table.
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
