import {
	OAuthClient,
	Key,
	type Session,
	type InternalStateData,
	type OAuthClientMetadataInput,
	type RuntimeLock
} from '@atproto/oauth-client';
import { Dexie, type EntityTable } from 'dexie';
import { WebcryptoKey } from '@atproto/oauth-client-browser';

const db = new Dexie('mini-shared-worker-db') as Dexie & {
	state: EntityTable<{ key: string; data: InternalStateData }, 'key'>;
	session: EntityTable<{ key: string; data: Session }, 'key'>;
};
db.version(1).stores({
	state: `key`,
	session: `key`
});

const requestLock: undefined | RuntimeLock = navigator.locks?.request
	? <T>(name: string, fn: () => T | PromiseLike<T>): Promise<T> =>
			navigator.locks.request(name, { mode: 'exclusive' }, async () => fn())
	: undefined;

type EncodedKey = { keyId: Key['kid']; keyPair: CryptoKeyPair };
function encodeKey(key: Key): EncodedKey {
	if (!(key instanceof WebcryptoKey) || !key.kid) {
		throw new Error('Invalid key object');
	}
	return {
		keyId: key.kid,
		keyPair: key.cryptoKeyPair
	};
}

async function decodeKey(encoded: EncodedKey): Promise<Key> {
	return WebcryptoKey.fromKeypair(encoded.keyPair, encoded.keyId);
}

export const workerOauthClient = (clientMetadata: OAuthClientMetadataInput) =>
	new OAuthClient({
		handleResolver: 'https://resolver.roomy.chat',
		responseMode: 'query',
		clientMetadata,

		runtimeImplementation: {
			// A runtime specific implementation of the crypto operations needed by the
			// OAuth client. See "@atproto/oauth-client-browser" for a browser specific
			// implementation. The following example is suitable for use in NodeJS.

			async createKey(algs: string[]): Promise<Key> {
				const key = await WebcryptoKey.generate(algs);
				console.log('key', key);
				return key;
			},

			getRandomValues(length: number): Uint8Array | PromiseLike<Uint8Array> {
				return crypto.getRandomValues(new Uint8Array(length));
			},

			async digest(bytes: Uint8Array, algorithm: { name: string }): Promise<Uint8Array> {
				// sha256 is required. Unsupported algorithms should throw an error.
				if (algorithm.name.startsWith('sha')) {
					const subtleAlgo = `SHA-${algorithm.name.slice(3)}`;
					const buffer = await crypto.subtle.digest(subtleAlgo, bytes);
					return new Uint8Array(buffer);
				}

				throw new TypeError(`Unsupported algorithm: ${algorithm.name}`);
			},

			requestLock
		},

		stateStore: {
			async set(key: string, internalState: InternalStateData): Promise<void> {
				console.log(internalState.dpopKey.algorithms);
				db.state.put({
					key,
					data: { ...internalState, dpopKey: encodeKey(internalState.dpopKey) as any }
				});
			},
			async get(key: string): Promise<InternalStateData | undefined> {
				const data = (await db.state.get(key))?.data;
				if (data) {
					data.dpopKey = await decodeKey(data.dpopKey as any);
				}
				return data;
			},
			async del(key: string): Promise<void> {
				await db.state.delete(key);
			}
		},

		sessionStore: {
			async set(sub: string, session: Session): Promise<void> {
				db.session.put({
					key: sub,
					data: { ...session, dpopKey: encodeKey(session.dpopKey) as any }
				});
			},
			async get(sub: string): Promise<Session | undefined> {
				const data = (await db.session.get(sub))?.data;
				if (data) {
					data.dpopKey = await decodeKey(data.dpopKey as any);
				}
				return data;
			},
			async del(sub: string): Promise<void> {
				await db.session.delete(sub);
			}
		},

		fetch,
		allowHttp: true
	});
