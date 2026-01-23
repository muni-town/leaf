import type { ProfileViewDetailed } from '@atproto/api/dist/client/types/app/bsky/actor/defs';
import { messagePortInterface, reactiveWorkerState } from './workerMessaging';
import backendWorkerUrl from './backendWorker.ts?worker&url';
import type { LeafQuery, SqlRows, BasicModule } from '@muni-town/leaf-client';

// Force page reload when hot reloading this file to avoid confusion if the workers get mixed up.
if (import.meta.hot) {
	import.meta.hot.accept(() => window.location.reload());
}

export interface BackendStatus {
	authLoaded: boolean | undefined;
	did: string | undefined;
	profile: ProfileViewDetailed | undefined;
	leafConnected: boolean | undefined;
}

/** Reactive status of the shared worker "backend". */
export const backendStatus = reactiveWorkerState<BackendStatus>(
	new BroadcastChannel('backend-status'),
	false
);

export type BackendInterface = {
	login(username: string): Promise<string>;
	logout(): Promise<void>;
	oauthCallback(searchParams: string): Promise<void>;
	getProfile(did?: string): Promise<ProfileViewDetailed | undefined>;
	query(streamDid: string, query: LeafQuery): Promise<SqlRows>;
	hasModule(moduleCid: string): Promise<boolean>;
	streamInfo(streamDid: string): Promise<{ moduleCid?: string }>;
  setHandle(StreamDid: string, handle: string | null): Promise<void>;
	createStream(moduleCid: string): Promise<{ streamDid: string }>;
	updateModule(streamDid: string, moduleCid: string): Promise<void>;
	subscribeEvents(streamDid: string, query: LeafQuery): Promise<string>;
	unsubscribe(subId: string): Promise<void>;
	uploadModule(module: BasicModule): Promise<{ moduleCid: string }>;
	sendEvents(streamDid: string, events: Uint8Array[]): Promise<void>;
	setLeafUrl(url: string): Promise<void>;
	/** Adds a new message port connection to the backend that can call the backend interface. */
	addClient(port: MessagePort): Promise<void>;
};

// Initialize shared worker
export const hasSharedWorker = 'SharedWorker' in globalThis;
const SharedWorkerConstructor = hasSharedWorker ? SharedWorker : Worker;
const backendWorker = new SharedWorkerConstructor(backendWorkerUrl, {
	name: 'leaf-explorer-backend',
	type: 'module'
});

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export const backend = messagePortInterface<{}, BackendInterface>(
	'port' in backendWorker ? backendWorker.port : backendWorker,
	{}
);

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(globalThis as any).backend = backend;
