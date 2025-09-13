/* eslint-disable @typescript-eslint/no-explicit-any */
/// <reference lib="webworker" />

import { LeafClient } from '@muni-town/leaf-client';
import type { BackendInterface, BackendStatus } from './index';
import { messagePortInterface, reactiveWorkerState, type MessagePortApi } from './workerMessaging';

import {
	atprotoLoopbackClientMetadata,
	BrowserOAuthClient,
	buildLoopbackClientId,
	type OAuthClientMetadataInput,
	type OAuthSession
} from '@atproto/oauth-client-browser';
import { Agent } from '@atproto/api';
import type { ProfileViewDetailed } from '@atproto/api/dist/client/types/app/bsky/actor/defs';
import Dexie, { type EntityTable } from 'dexie';

/**
 * Check whether or not we are executing in a shared worker.
 *
 * On platforms like Android chrome where SharedWorkers are not available this script will run as a
 * dedicated worker instead of a shared worker.
 * */
const isSharedWorker = 'SharedWorkerGlobalScope' in globalThis;

const status = reactiveWorkerState<BackendStatus>(new BroadcastChannel('backend-status'), true);

const atprotoOauthScope = 'atproto transition:generic transition:chat.bsky';

interface KeyValue {
	key: string;
	value: string;
}
const db = new Dexie('mini-shared-worker-db') as Dexie & {
	kv: EntityTable<KeyValue, 'key'>;
};
db.version(1).stores({
	kv: `key,value`
});

// TODO: This might be a horrible local storage shim. I don't know how it handles multiple tabs
// open. Works for now... 🤞 We just need it so that the atproto/oauth-client-browser doesn't panic
// because localStorage isn't defined.
globalThis.localStorage = {
	data: {} as { [key: string]: string },
	persist() {
		db.kv.put({ key: 'localStorageShim', value: JSON.stringify(this.data) });
	},
	clear() {
		this.data = {};
	},
	getItem(s: string): string | null {
		return this.data[s] || null;
	},
	key(idx: number): string | null {
		return (Object.values(this.data)[idx] as string | undefined) || null;
	},
	get length(): number {
		return Object.values(this.data).length;
	},
	removeItem(key: string) {
		this.data[key] = undefined;
		this.persist();
	},
	setItem(key: string, value: string) {
		this.data[key] = value;
		this.persist();
	}
};

/**
 * Helper class wrapping up our worker state behind getters and setters so we run code whenever
 * they are changed.
 * */
class Backend {
	#oauth: BrowserOAuthClient | undefined;
	#agent: Agent | undefined;
	#session: OAuthSession | undefined;
	#profile: ProfileViewDetailed | undefined;
	#leafClient: LeafClient | undefined;
	#leafUrl: string | undefined;

	#oauthReady: Promise<void>;
	#resolveOauthReady: () => void = () => {};
	get ready() {
		return state.#oauthReady;
	}

	constructor() {
		this.#oauthReady = new Promise((r) => (this.#resolveOauthReady = r));
		createOauthClient().then((client) => {
			this.setOauthClient(client);
		});
	}

	get oauth() {
		return this.#oauth;
	}

	setOauthClient(oauth: BrowserOAuthClient) {
		this.#oauth = oauth;

		if (oauth) {
			(async () => {
				// if there's a stored DID and no session yet, try to restore the session
				const entry = await db.kv.get('did');
				if (entry && this.oauth && !this.session) {
					try {
						const restoredSession = await this.oauth.restore(entry.value);
						this.setSession(restoredSession);
					} catch (e) {
						console.error(e);
						this.logout();
					}
				}
				this.#resolveOauthReady();
				status.authLoaded = true;
			})();
		} else {
			this.setSession(undefined);
		}
	}

	get session() {
		return this.#session;
	}

	setSession(session: OAuthSession | undefined) {
		this.#session = session;
		status.did = session?.did;
		if (session) {
			db.kv.add({ key: 'did', value: session.did });
			this.setAgent(new Agent(session));
		} else {
			this.setAgent(undefined);
		}
	}

	get agent() {
		return this.#agent;
	}

	setAgent(agent: Agent | undefined) {
		this.#agent = agent;
		if (agent) {
			agent.getProfile({ actor: agent.assertDid }).then((resp) => {
				this.profile = resp.data;
			});

			if (!this.#leafClient && this.#leafUrl) {
				this.setLeafClient(createLeafClient(agent, this.#leafUrl));
			}
		} else {
			this.profile = undefined;
			this.#leafClient?.disconnect();
			this.setLeafClient(undefined);
		}
	}

	get profile() {
		return this.#profile;
	}
	set profile(profile) {
		this.#profile = profile;
		status.profile = profile;
	}

	get leafUrl() {
		return this.#leafUrl;
	}
	setLeafUrl(url: string) {
		this.#leafUrl = url;
		if (this.#agent) {
			this.setLeafClient(createLeafClient(this.#agent, this.#leafUrl));
		}
	}

	get leafClient() {
		return this.#leafClient;
	}
	setLeafClient(client: LeafClient | undefined) {
		if (client) {
			this.#leafClient?.disconnect();
			initializeLeafClient(client);
		} else {
			this.#leafClient?.disconnect();
		}
		this.#leafClient = client;
	}

	async oauthCallback(params: URLSearchParams) {
		await this.#oauthReady;
		const response = await state.oauth?.callback(params);
		this.setSession(response?.session);
	}

	logout() {
		db.kv.delete('did');
		this.setSession(undefined);
	}
}

const state = new Backend();
(globalThis as any).state = state;

if (isSharedWorker) {
	(globalThis as any).onconnect = async ({ ports: [port] }: { ports: [MessagePort] }) => {
		connectMessagePort(port);
	};
} else {
	connectMessagePort(globalThis);
}

function connectMessagePort(port: MessagePortApi) {
	// eslint-disable-next-line @typescript-eslint/no-empty-object-type
	messagePortInterface<BackendInterface, {}>(port, {
		async getProfile(did) {
			await state.ready;
			if (!did || did == state.agent?.did) {
				return state.profile;
			}
			const resp = await state.agent?.getProfile({ actor: did || state.agent.assertDid });
			if (!resp?.data && !resp?.success) {
				console.error('error fetching profile', resp, state.agent);
				throw new Error('Error fetching profile:' + resp?.toString());
			}
			return resp.data;
		},
		async login(handle) {
			if (!state.oauth) throw 'OAuth not initialized';
			const url = await state.oauth.authorize(handle, {
				scope: atprotoOauthScope
			});
			return url.href;
		},
		async oauthCallback(paramsStr) {
			const params = new URLSearchParams(paramsStr);
			await state.oauthCallback(params);
		},
		async setLeafUrl(url) {
			state.setLeafUrl(url);
		},
		async logout() {
			state.logout();
		},
		async sendEvent(streamId, payload) {
			await state.leafClient?.sendEvent(streamId, payload);
		},
		async hasModule(moduleId) {
			return (await state.leafClient?.hasModule(moduleId)) || false;
		},
		async uploadModule(buffer) {
			return await state.leafClient!.uploadModule(buffer);
		},
		async createStream(moduleId: string, params: ArrayBuffer) {
			return await state.leafClient!.createStream(moduleId, params);
		},
		async subscribe(streamId) {
			state.leafClient?.subscribe(streamId);
		},
		async unsubscribe(streamId) {
			state.leafClient?.unsubscribe(streamId);
		},
		async fetchEvents(streamId, offset, limit) {
			if (!state.leafClient) throw 'Leaf client not initialized';
			const events = (await state.leafClient?.fetchEvents(streamId, { offset, limit }))?.map(
				(x) => ({
					...x,
					stream: streamId
				})
			);
			return events;
		},
		async addClient(port) {
			connectMessagePort(port);
		}
	});
}

function createLeafClient(agent: Agent, url: string) {
	return new LeafClient(url, async () => {
		const resp = await agent.com.atproto.server.getServiceAuth({
			aud: `did:web:${new URL(url).host}`
		});
		if (!resp) throw 'Error authenticating for leaf server';
		return resp.data.token;
	});
}

async function createOauthClient(): Promise<BrowserOAuthClient> {
	// Build the client metadata
	let clientMetadata: OAuthClientMetadataInput;
	if (import.meta.env.DEV) {
		// Get the base URL and redirect URL for this deployment
		if (globalThis.location.hostname == 'localhost')
			throw new Error('Logging in only works from 127.0.0.1');
		const baseUrl = new URL(`http://127.0.0.1:${globalThis.location.port}`);
		baseUrl.hash = '';
		baseUrl.pathname = '/';
		const redirectUri = baseUrl.href + 'oauth/callback';
		// In dev, we build a development metadata
		clientMetadata = {
			...atprotoLoopbackClientMetadata(buildLoopbackClientId(baseUrl)),
			redirect_uris: [redirectUri],
			scope: atprotoOauthScope,
			client_id: `http://localhost?redirect_uri=${encodeURIComponent(
				redirectUri
			)}&scope=${encodeURIComponent(atprotoOauthScope)}`
		};
	} else {
		// In prod, we fetch the `/oauth-client.json` which is expected to be deployed alongside the
		// static build.
		// native client metadata is not reuqired to be on the same domin as client_id,
		// so it can always use the deployed metadata
		const resp = await fetch(`/oauth-client.json`, {
			headers: [['accept', 'application/json']]
		});
		clientMetadata = await resp.json();
	}

	return new BrowserOAuthClient({
		responseMode: 'query',
		handleResolver: 'https://resolver.roomy.chat',
		clientMetadata
	});
}

const eventBroadcast = new BroadcastChannel('leaf-events');
async function initializeLeafClient(client: LeafClient) {
	status.leafConnected = false;
	const url = state.leafUrl;
	client.on('connect', async () => {
		console.log('Leaf: connected', url);
		if (url == state.leafUrl) status.leafConnected = true;
	});
	client.on('disconnect', () => {
		console.log('Leaf: disconnected', url);
		if (url == state.leafUrl) status.leafConnected = false;
	});
	client.on('authenticated', (did) => {
		console.log('Leaf: authenticated as', did, url);
	});
	client.on('event', (event) => {
		eventBroadcast.postMessage(event);
	});
}
