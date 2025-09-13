<script lang="ts">
	import '../app.css';
	import favicon from '$lib/assets/favicon.svg';
	import { backend, backendStatus } from '$lib/workers';
	import { onMount, setContext } from 'svelte';
	import { page } from '$app/state';
	import type { IncomingEvent } from '@muni-town/leaf-client';

	let leafUrl = $state(localStorage.getItem('leaf-url') || 'https://leaf-dev.muni.town');
	onMount(() => {
		backend.setLeafUrl(leafUrl);
	});
	let loginHandle = $state('');
	let loginLoading = $state(false);

	const leafEventsChanel = new BroadcastChannel('leaf-events');
	leafEventsChanel.onmessage = (ev) => {
		const event: IncomingEvent = ev.data;

		leafEvents.push(
			`sub ${event.idx}(${event.user}) - ${event.stream}\n    ${new TextDecoder().decode(event.payload)}`
		);
	};
	let leafEvents = $state([]) as string[];
	let streamId = $state(localStorage.getItem('stream-id') || '');
	$effect(() => {
		localStorage.setItem('stream-id', streamId);
	});

	setContext('events', leafEvents);
	setContext('streamId', {
		get value() {
			return streamId;
		},
		set value(v) {
			streamId = v;
		}
	});

	// Check for oauth callback
	let oauthCallbackError = $state(undefined) as undefined | string;
	let isOauthCallback = $derived(page.url.pathname == '/oauth/callback');
	onMount(async () => {
		if (!isOauthCallback) return;
		const searchParams = new URL(globalThis.location.href).searchParams;

		backend
			.oauthCallback(searchParams.toString())
			.then(() => {
				window.location.href = '/';
			})
			.catch((e) => {
				oauthCallbackError = e.toString();
			});
	});

	let { children } = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

<div class="flex h-screen max-h-full flex-col">
	<div class="px-5 py-3">
		<div class="navbar bg-base-100 flex gap-8 p-3 shadow-md">
			<a class="text-xl" href="/">Leaf Explorer</a>

			{#if backendStatus.did}
				<input class="input" bind:value={leafUrl} />
				<button
					class="btn btn-outline"
					onclick={() => {
						backend.setLeafUrl(leafUrl);
						localStorage.setItem('leaf-url', leafUrl);
					}}>Connect</button
				>
			{/if}

			<div>
				{#if backendStatus.authLoaded}
					{#if backendStatus.did}
						<span
							class={backendStatus.leafConnected ? 'text-green-700' : 'text-red-700'}
						>
							{backendStatus.leafConnected ? 'Connected' : 'Disconnected'}
						</span>
					{/if}
				{/if}
			</div>
			<div class="grow"></div>
			{#if backendStatus.profile}
				<button class="cursor-pointer" title="logout" onclick={() => backend.logout()}>
					<div class="flex items-center gap-3">
						{backendStatus.profile.handle}
						<img
							class="w-10 rounded-full"
							alt="avatar"
							src={backendStatus.profile.avatar}
						/>
					</div>
				</button>
			{:else if backendStatus.authLoaded && !backendStatus.did}
				Not logged in
			{/if}
		</div>
	</div>

	{#if backendStatus.did}
		<div class="mb-3 px-5">
			<label class="flex items-center gap-2">
				Stream ID
				<input class="input" bind:value={streamId} />
				<button class="btn" onclick={() => backend.subscribe(streamId)}>Subscribe</button>
				<button class="btn" onclick={() => backend.unsubscribe(streamId)}
					>Unsubscribe</button
				>
				<div class="grow"></div>
				<button
					class="btn"
					onclick={() => {
						leafEvents.splice(0, leafEvents.length);
					}}>Clear Log</button
				>
			</label>
		</div>

		<div class="flex min-h-0 min-w-0 shrink flex-row gap-3 px-5">
			<div
				class="border-accent bg-base-100 thin-scroll w-[24em] shrink overflow-y-auto shadow-md"
			>
				{@render children?.()}
			</div>
			<pre
				class="bg-base-100 thin-scroll min-w-[20em] grow overflow-auto shadow-md">{leafEvents
					.map((x) => x)
					.join('\n')}</pre>
		</div>
	{:else if oauthCallbackError}
		<div class="flex h-full w-full grow items-center justify-center">
			<div role="alert" class="alert alert-error">
				<svg
					xmlns="http://www.w3.org/2000/svg"
					class="h-6 w-6 shrink-0 stroke-current"
					fill="none"
					viewBox="0 0 24 24"
				>
					<path
						stroke-linecap="round"
						stroke-linejoin="round"
						stroke-width="2"
						d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z"
					/>
				</svg>
				<span>Error logging in: {oauthCallbackError}</span>
			</div>
		</div>
	{:else if backendStatus.authLoaded && !isOauthCallback}
		<div class="flex h-full w-full grow items-center justify-center">
			<form
				class="card bg-base-100 flex flex-col gap-6 p-8 shadow-md"
				onsubmit={async () => {
					loginLoading = true;
					window.location.href = await backend.login(loginHandle);
				}}
			>
				<h1 class="text-2xl font-bold">Login</h1>

				<label class="flex flex-col gap-1">
					Bluesky / ATProto Handle
					<input class="input" bind:value={loginHandle} />
				</label>
				<button type="submit" class="btn btn-primary" disabled={loginLoading}
					>{loginLoading ? 'Loading...' : 'Login'}</button
				>
			</form>
		</div>
	{:else}
		<div class="flex grow items-center justify-center">
			<svg
				class="h-40 animate-spin"
				viewBox="0 0 24 24"
				fill="none"
				xmlns="http://www.w3.org/2000/svg"
				><g id="SVGRepo_bgCarrier" stroke-width="0"></g><g
					id="SVGRepo_tracerCarrier"
					stroke-linecap="round"
					stroke-linejoin="round"
				></g><g id="SVGRepo_iconCarrier">
					<path
						d="M20.0001 12C20.0001 13.3811 19.6425 14.7386 18.9623 15.9405C18.282 17.1424 17.3022 18.1477 16.1182 18.8587C14.9341 19.5696 13.5862 19.9619 12.2056 19.9974C10.825 20.0328 9.45873 19.7103 8.23975 19.0612"
						stroke="#aaaaaa"
						stroke-width="3.55556"
						stroke-linecap="round"
					></path>
				</g></svg
			>
		</div>
	{/if}
</div>

<style>
	.thin-scroll {
		scrollbar-width: thin;
	}
</style>
