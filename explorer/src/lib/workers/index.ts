import type { ProfileViewDetailed } from '@atproto/api/dist/client/types/app/bsky/actor/defs';
import { messagePortInterface, reactiveWorkerState } from './workerMessaging';
import backendWorkerUrl from './backendWorker.ts?worker&url';
import type {
	BasicModuleDef,
	IncomingEvent,
	LeafQuery,
	SqlRows,
	StreamGenesis
} from '@muni-town/leaf-client';

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
	query(streamId: string, query: LeafQuery): Promise<SqlRows>;
	hasModule(moduleId: string): Promise<boolean>;
	createStream(genesis: StreamGenesis): Promise<string>;
	updateModule(streamId: string, moduleId: string): Promise<void>;
	subscribe(streamId: string, query: LeafQuery): Promise<string>;
	unsubscribe(subId: string): Promise<void>;
	uploadModule(buffer: BasicModuleDef): Promise<string>;
	sendEvents(streamId: string, events: IncomingEvent[]): Promise<void>;
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
