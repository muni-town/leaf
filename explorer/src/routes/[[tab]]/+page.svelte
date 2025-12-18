<script lang="ts">
	import EventList from '$lib/components/EventList.svelte';
	import CodeMirror from 'svelte-codemirror-editor';
	import { sql as sqlLang } from '@codemirror/lang-sql';
	import { json as jsonLang } from '@codemirror/lang-json';
	import { oneDark as oneDarkTheme } from '@codemirror/theme-one-dark';

	import { backend, backendStatus } from '$lib/workers';
	import { getContext } from 'svelte';
	import { BytesWrapper, type BasicModule, type LeafQuery } from '@muni-town/leaf-client';
	import { page } from '$app/state';
	import { encode, decode } from '@atcute/cbor';

	let loading = $state(false);

	const events = getContext<string[]>('events');
	const streamDid = getContext<{ value: string }>('streamId');

	let offset = $state(1);
	let limit = $state(100);

	let moduleId = $state(localStorage.getItem('module') || '');
	$effect(() => {
		localStorage.setItem('module', moduleId);
	});

	const tabs = ['Query', 'Create Stream'] as const;
	let currentTab = $derived(page.params.tab || '' in tabs ? page.params.tab : 'Query');

	let payload = $state(localStorage.getItem('payload') || '');
	$effect(() => {
		localStorage.setItem('payload', payload);
	});

	const defaultModule: Omit<BasicModule, '$type'> = {
		authorizer: '',
		initSql: '',
		materializer: '',
		queries: []
	};
	let newStreamModule: Omit<BasicModule, '$type'> = $state(
		JSON.parse(localStorage.getItem('basicModule') || JSON.stringify(defaultModule))
	);
	$effect(() => {
		localStorage.setItem('basicModule', JSON.stringify(newStreamModule || defaultModule));
	});

	const defaultQuery: LeafQuery = {
		name: '',
		user: '',
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
		console.log('module', $state.snapshot(newStreamModule));
		const resp = await backend.uploadModule({
			...$state.snapshot(newStreamModule),
			$type: 'muni.town.leaf.module.basic.v0'
		});
		console.log('moduleCid', resp.moduleCid);
		streamDid.value = (await backend.createStream(resp.moduleCid)).streamDid;
		console.log('streamDid', streamDid.value);
	}
	async function updateModule() {
		if (!backendStatus.did) return;
		const moduleCid = (
			await backend.uploadModule({
				...$state.snapshot(newStreamModule),
				$type: 'muni.town.leaf.module.basic.v0'
			})
		).moduleCid;
		await backend.updateModule(streamDid.value, moduleCid);
	}

	async function runQuery() {
		if (!backendStatus.did) return;
		const q: typeof query = $state.snapshot(query) as any;
		q.user = backendStatus.did;
		for (const [_name, param] of q.params) {
			// The forms don't assign the proper data types to non-text params, so we convert them here.
			if (param.$type == 'muni.town.sqliteValue.blob') {
				param.value = new BytesWrapper(new TextEncoder().encode(param.value as any));
			} else if (param.$type == 'muni.town.sqliteValue.integer') {
				param.value = parseInt(param.value as any);
			} else if (param.$type == 'muni.town.sqliteValue.real') {
				param.value = parseFloat(param.value as any);
			}
		}
		const result = await backend.query(streamDid.value, q);
		events.push(
			JSON.stringify(
				result,
				(_key, value) => {
					if (typeof value === 'bigint') {
						return value.toString();
					} else if (typeof value == 'object') {
						if (value.$type == 'muni.town.sqliteValue.blob') {
							try {
								return decode(value.value);
							} catch (_e) {
								return `Uint8Array(${value.value.length})`;
							}
						} else if (value.$type) {
							return value.value;
						}
					}
					return value;
				},
				'  '
			)
		);
	}

	async function subscribe() {
		if (!backendStatus.did) return;
		const q: typeof query = $state.snapshot(query) as any;
		q.user = backendStatus.did;
		for (const [_name, param] of q.params) {
			// The forms don't assign the proper data types to non-text params, so we convert them here.
			if (param.$type == 'muni.town.sqliteValue.blob') {
				param.value = new BytesWrapper(new TextEncoder().encode(param.value as any));
			} else if (param.$type == 'muni.town.sqliteValue.integer') {
				param.value = parseInt(param.value as any);
			} else if (param.$type == 'muni.town.sqliteValue.real') {
				param.value = parseFloat(param.value as any);
			}
		}
		subscriptionId = await backend.subscribe(streamDid.value, q);
	}

	async function unsubscribe() {
		if (!subscriptionId) throw 'no subscription';
		await backend.unsubscribe(subscriptionId);
		subscriptionId = '';
	}

	async function sendEvent() {
		if (!backendStatus.did) return;
		await backend.sendEvents(streamDid.value, [encode(JSON.parse(payload))]);
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
				<input class="input" placeholder="query name" bind:value={query.name} />
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
									$type: 'muni.town.sqliteValue.text',
									value: ''
								}
							]);
						}}>+</button
					>
				</h3>

				{#each query.params as [_name, value], i}
					<div class="flex items-center gap-3">
						<input class="input" bind:value={query.params[i][0]} placeholder="name" />
						<select class="select" bind:value={query.params[i][1].$type}>
							<option value="muni.town.sqliteValue.null">Null</option>
							<option value="muni.town.sqliteValue.integer">Integer</option>
							<option value="muni.town.sqliteValue.real">Real</option>
							<option value="muni.town.sqliteValue.text">Text</option>
							<option value="muni.town.sqliteValue.blob">Blob</option>
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
				<input class="input w-full" bind:value={moduleId} placeholder="module CID" />
				<button class="btn btn-outline" disabled={loading}>Check</button>
			</form>

			<!-- Stream Info -->
			<form
				class="m-8 flex flex-col gap-2"
				onsubmit={async () => {
					loading = true;
					try {
						const streamInfo = await backend.streamInfo(streamDid.value);
						events.push(`Stream info: module Cid: ${streamInfo.moduleCid}`);
					} catch (e: any) {
						events.push(e.toString());
					}
					loading = false;
				}}
			>
				<h2 class="mb-4 text-xl font-bold">StreamInfo</h2>
				<button class="btn btn-outline" disabled={loading}>Get Info</button>
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
					bind:value={newStreamModule.initSql}
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
					bind:value={newStreamModule.authorizer}
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
					bind:value={newStreamModule.materializer}
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
							newStreamModule.queries.push({
								name: '',
								sql: '',
								params: []
							});
						}}>+</button
					>
				</h2>
				<hr class="my-3" />
				{#each newStreamModule.queries as query, i}
					<div class="m-8 flex flex-col gap-3">
						<div class="flex gap-3">
							<input class="input" placeholder="query name" bind:value={query.name} />
							<button
								class="btn"
								onclick={() =>
									(newStreamModule.queries = newStreamModule.queries.splice(
										i,
										0
									))}>Delete Query</button
							>
						</div>
						<div class="m-3 gap-2 text-sm opacity-40">
							<p>SQL used to return the query results.</p>
							<p>
								To access the user that is making the query you can use the <code
									>$requesting_user</code
								>
								placeholder, as well as and
								<code>$start</code> and <code>$limit</code> in order to limit results
								based on the event index and count.
							</p>
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
										kind: 'any',
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
