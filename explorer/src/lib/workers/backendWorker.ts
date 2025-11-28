/* eslint-disable @typescript-eslint/no-explicit-any */
/// <reference lib="webworker" />

import { LeafClient } from '@muni-town/leaf-client';
import type { BackendInterface, BackendStatus } from './index';
import { messagePortInterface, reactiveWorkerState, type MessagePortApi } from './workerMessaging';

import {
	atprotoLoopbackClientMetadata,
	buildLoopbackClientId,
	OAuthClient,
	type OAuthClientMetadataInput,
	type OAuthSession
} from '@atproto/oauth-client-browser';
import { Agent } from '@atproto/api';
import type { ProfileViewDetailed } from '@atproto/api/dist/client/types/app/bsky/actor/defs';
import Dexie, { type EntityTable } from 'dexie';
import { workerOauthClient } from './oauth';
import { ulid } from 'ulidx';

/**
 * Check whether or not we are executing in a shared worker.
 *
 * On platforms like Android chrome where SharedWorkers are not available this script will run as a
 * dedicated worker instead of a shared worker.
 * */
const isSharedWorker = 'SharedWorkerGlobalScope' in globalThis;

const status = reactiveWorkerState<BackendStatus>(new BroadcastChannel('backend-status'), true);
(globalThis as any).status = status;

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

/**
 * Helper class wrapping up our worker state behind getters and setters so we run code whenever
 * they are changed.
 * */
class Backend {
	#oauth: OAuthClient | undefined;
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

	setOauthClient(oauth: OAuthClient) {
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

const subscriptionBroadcast = new BroadcastChannel('leaf-events');
function connectMessagePort(port: MessagePortApi) {
	const unsubscribers: Map<string, () => Promise<void>> = new Map();

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
		async sendEvents(streamId, events) {
			await state.leafClient?.sendEvents(streamId, events);
		},
		async hasModule(moduleId) {
			return (await state.leafClient?.hasModule(moduleId)) || false;
		},
		async streamInfo(streamId) {
			if (!state.leafClient) throw new Error('Leaf not connected');
			return await state.leafClient.streamInfo(streamId);
		},
		async uploadModule(module) {
			console.log('uploading module');
			const moduleId = await state.leafClient!.uploadBasicModule(module);
      console.log('Created module:', moduleId);
      return moduleId;
		},
		async createStream(genesis) {
			if (!state.leafClient) throw new Error('Leaf not connected');
      console.log("Creating stream:", genesis);
			const streamId = await state.leafClient.createStream(genesis);
      console.log('created stream:', streamId);
      return streamId;
		},
		async updateModule(streamId, moduleDef) {
			if (!state.leafClient) throw new Error('Leaf not connected');
			await state.leafClient.updateModule(streamId, moduleDef);
		},
		async subscribe(streamId, query) {
			if (!state.leafClient) throw 'No leaf client';
			const subId = ulid();
			const unsub = await state.leafClient.subscribe(streamId, query, (resp) => {
				subscriptionBroadcast.postMessage({ subId, resp });
			});
			unsubscribers.set(subId, unsub);

			return subId;
		},
		async unsubscribe(subId) {
			await unsubscribers.get(subId)?.();
			unsubscribers.delete(subId);
		},
		async query(streamId, query) {
			if (!state.leafClient) throw 'Leaf client not initialized';
			console.log('Query', query);
			const resp = await state.leafClient.query(streamId, query);
			return resp;
		},
		async addClient(port) {
			connectMessagePort(port);
		}
	});
}

function createLeafClient(agent: Agent, url: string) {
	return new LeafClient(url, async () => {
		const resp = await agent.com.atproto.server.getServiceAuth({
			aud: `did:web:${new URL(url).hostname}`
		});
		if (!resp) throw 'Error authenticating for leaf server';
		return resp.data.token;
	});
}

async function createOauthClient(): Promise<OAuthClient> {
	// Build the client metadata
	let clientMetadata: OAuthClientMetadataInput;
	if (import.meta.env.DEV) {
		// Get the base URL and redirect URL for this deployment
		if (globalThis.location.hostname == 'localhost')
			throw new Error('Logging in only works from 127.0.0.1');
		const baseUrl = new URL(`http://127.0.0.1:${globalThis.location.port}`);
		baseUrl.hash = '';
		baseUrl.pathname = '/';
		const redirectUri = baseUrl.href;
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

	return workerOauthClient(clientMetadata);
}

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
}
