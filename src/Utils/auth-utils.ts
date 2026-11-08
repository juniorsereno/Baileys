import NodeCache from '@cacheable/node-cache'
import { Boom } from '@hapi/boom'
import { AsyncLocalStorage } from 'async_hooks'
import { Mutex } from 'async-mutex'
import { randomBytes } from 'crypto'
import { DEFAULT_CACHE_TTLS } from '../Defaults'
import type {
	AuthenticationCreds,
	CacheStore,
	SignalDataSet,
	SignalDataTypeMap,
	SignalKeyStore,
	SignalKeyStoreWithRecordTransaction,
	TransactionCapabilityOptions
} from '../Types'
import { Curve, signedKeyPair } from './crypto'
import { delay, generateRegistrationId } from './generics'
import { type LockManager, type LockRef, lockRefKey, makeLockManager } from './lock-manager'
import type { ILogger } from './logger'
import { PreKeyManager } from './pre-key-manager'

interface TransactionContext {
	cache: SignalDataSet
	mutations: SignalDataSet
	dbQueries: number
	leases: Map<string, Promise<() => void>>
	nestedLocks: LockManager
	sealed: boolean
}

/**
 * Adds caching capability to a SignalKeyStore
 * @param store the store to add caching to
 * @param logger to log trace events
 * @param _cache cache store to use
 */
export function makeCacheableSignalKeyStore(
	store: SignalKeyStore,
	logger?: ILogger,
	_cache?: CacheStore
): SignalKeyStore {
	const cache =
		_cache ||
		new NodeCache<SignalDataTypeMap[keyof SignalDataTypeMap]>({
			stdTTL: DEFAULT_CACHE_TTLS.SIGNAL_STORE, // 5 minutes
			useClones: false,
			deleteOnExpire: true
		})

	// Mutex for protecting cache operations
	const cacheMutex = new Mutex()

	function getUniqueId(type: string, id: string) {
		return `${type}.${id}`
	}

	return {
		async get(type, ids) {
			return cacheMutex.runExclusive(async () => {
				const data: { [_: string]: SignalDataTypeMap[typeof type] } = {}
				const idsToFetch: string[] = []

				for (const id of ids) {
					const item = (await cache.get<SignalDataTypeMap[typeof type]>(getUniqueId(type, id))) as any
					if (typeof item !== 'undefined') {
						data[id] = item
					} else {
						idsToFetch.push(id)
					}
				}

				if (idsToFetch.length) {
					logger?.trace({ items: idsToFetch.length }, 'loading from store')
					const fetched = await store.get(type, idsToFetch)
					for (const id of idsToFetch) {
						const item = fetched[id]
						if (item) {
							data[id] = item
							// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
							await cache.set(getUniqueId(type, id), item as SignalDataTypeMap[keyof SignalDataTypeMap])
						}
					}
				}

				return data
			})
		},
		async set(data) {
			return cacheMutex.runExclusive(async () => {
				await store.set(data)
				let keys = 0
				for (const type in data) {
					for (const id in data[type as keyof SignalDataTypeMap]) {
						await cache.set(getUniqueId(type, id), data[type as keyof SignalDataTypeMap]![id]!)
						keys += 1
					}
				}

				logger?.trace({ keys }, 'updated cache')
			})
		},
		async clear() {
			await cache.flushAll()
			await store.clear?.()
		}
	}
}

/** Record locks cover validation, nested work and the outer durable commit. */
export const addTransactionCapability = (
	state: SignalKeyStore,
	logger: ILogger,
	{ maxCommitRetries, delayBetweenTriesMs }: TransactionCapabilityOptions
): SignalKeyStoreWithRecordTransaction => {
	const txStorage = new AsyncLocalStorage<TransactionContext>()
	const scopeStorage = new AsyncLocalStorage<{ held: ReadonlySet<string>; owner?: object }>()
	const locks = makeLockManager()
	const preKeyManager = new PreKeyManager(state, logger)

	/**
	 * Check if currently in a transaction
	 */
	function isInTransaction(): boolean {
		return !!txStorage.getStore()
	}

	/**
	 * Commit transaction with retries
	 */
	async function commitWithRetry(mutations: SignalDataSet): Promise<void> {
		if (Object.keys(mutations).length === 0) {
			logger.trace('no mutations in transaction')
			return
		}

		logger.trace('committing transaction')

		for (let attempt = 0; attempt < maxCommitRetries; attempt++) {
			try {
				await state.set(mutations)
				logger.trace({ mutationCount: Object.keys(mutations).length }, 'committed transaction')
				return
			} catch (error) {
				const retriesLeft = maxCommitRetries - attempt - 1
				logger.warn(`failed to commit mutations, retries left=${retriesLeft}`)

				if (retriesLeft === 0) {
					throw error
				}

				await delay(delayBetweenTriesMs)
			}
		}
	}

	async function retainLocks(ctx: TransactionContext, refs: readonly LockRef[]) {
		if (ctx.sealed) throw new Error('Transaction is already complete')
		const unique = new Map(refs.map(ref => [lockRefKey(ref), ref]))
		for (const [key, ref] of [...unique].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
			let lease = ctx.leases.get(key)
			if (!lease) {
				lease = locks.acquire(ref, ctx).then(release => {
					if (ctx.sealed) {
						release()
						throw new Error('Transaction completed while acquiring a lock')
					}

					return release
				})
				ctx.leases.set(key, lease)
			}

			await lease
		}
	}

	async function runTransaction<T>(refs: readonly LockRef[], work: () => Promise<T>): Promise<T> {
		const existing = txStorage.getStore()
		if (existing) {
			await retainLocks(existing, refs)
			const scope = scopeStorage.getStore()
			const held = scope?.held ?? new Set<string>()
			const owner = scope?.owner ?? {}
			const additional = refs.filter(ref => !held.has(lockRefKey(ref)))
			return existing.nestedLocks.withLocks(
				additional,
				() => scopeStorage.run({ held: new Set([...held, ...additional.map(lockRefKey)]), owner }, work),
				owner
			)
		}

		const ctx: TransactionContext = {
			cache: {},
			mutations: {},
			dbQueries: 0,
			leases: new Map(),
			nestedLocks: makeLockManager(),
			sealed: false
		}
		try {
			await retainLocks(ctx, refs)
			const result = await txStorage.run(ctx, () => scopeStorage.run({ held: new Set(refs.map(lockRefKey)) }, work))
			ctx.sealed = true
			await commitWithRetry(ctx.mutations)
			return result
		} catch (err) {
			logger.error({ err }, 'transaction failed, rolling back')
			throw err
		} finally {
			ctx.sealed = true
			// Release fulfilled leases immediately, including when sibling work is still unwinding.
			await Promise.all(
				[...ctx.leases.values()].map(async lease => {
					try {
						const release = await lease
						release()
					} catch {
						/* Failed acquisitions own no lock. */
					}
				})
			)
		}
	}

	return {
		get: async (type, ids) => {
			const ctx = txStorage.getStore()

			if (!ctx) {
				// No transaction - direct read without exclusive lock for concurrency
				return state.get(type, ids)
			}

			await retainLocks(
				ctx,
				ids.map(id => ({ namespace: type, id }))
			)

			// In transaction - check cache first
			const cached = ctx.cache[type] || {}
			const missing = ids.filter(id => !(id in cached))

			if (missing.length > 0) {
				ctx.dbQueries++
				logger.trace({ type, count: missing.length }, 'fetching missing keys in transaction')

				// Record locks stay held until this snapshot has been committed.
				const fetched = await state.get(type, missing)

				// Update cache
				ctx.cache[type] = ctx.cache[type] || ({} as any)
				Object.assign(ctx.cache[type]!, fetched)
			}

			// Return requested ids from cache
			const result: { [key: string]: any } = {}
			for (const id of ids) {
				const value = ctx.cache[type]?.[id]
				if (value !== undefined && value !== null) {
					result[id] = value
				}
			}

			return result
		},

		set: async data => {
			const ctx = txStorage.getStore()

			if (!ctx) {
				// Validate and persist all affected records under the same locks as transactions.
				const types = Object.keys(data) as Array<keyof SignalDataTypeMap>
				const lockRefs = types.flatMap(type => Object.keys(data[type] ?? {}).map(id => ({ namespace: type, id })))
				await locks.withLocks(lockRefs, async () => {
					if ('pre-key' in data) {
						await preKeyManager.validateDeletions(data, 'pre-key')
					}

					// `validateDeletions` may have emptied the pre-key bucket
					// (every targeted id was already gone). Skip `state.set` if
					// every bucket is empty — no work to do, and writing
					// `{ 'pre-key': {} }` is a wasteful no-op against the
					// storage adapter. Preserved from Stage 1's review fix.
					let hasAny = false
					for (const t of types) {
						const bucket = data[t]
						if (bucket && Object.keys(bucket).length > 0) {
							hasAny = true
							break
						}
					}

					if (hasAny) {
						await state.set(data)
					}
				})
				return
			}

			await retainLocks(
				ctx,
				(Object.keys(data) as Array<keyof SignalDataTypeMap>).flatMap(type =>
					Object.keys(data[type] ?? {}).map(id => ({ namespace: type, id }))
				)
			)

			// In transaction - update cache and mutations
			logger.trace({ types: Object.keys(data) }, 'caching in transaction')

			for (const key_ in data) {
				const key = key_ as keyof SignalDataTypeMap

				// Ensure structures exist
				ctx.cache[key] = ctx.cache[key] || ({} as any)
				ctx.mutations[key] = ctx.mutations[key] || ({} as any)

				// Special handling for pre-keys
				if (key === 'pre-key') {
					await preKeyManager.processOperations(data, key, ctx.cache, ctx.mutations, true)
				} else {
					// Normal key types
					Object.assign(ctx.cache[key]!, data[key])
					Object.assign(ctx.mutations[key]!, data[key])
				}
			}
		},

		isInTransaction,

		transaction: (work, key) => runTransaction([{ namespace: '__legacy__', id: key }], work),
		transactWith: (scope, work) =>
			runTransaction(
				scope.records.map(r => ({ namespace: r.type, id: r.id })),
				work
			)
	}
}

/**
 * Returns the authenticated user's JID, or throws a Boom-401 if creds are not yet authenticated.
 * Use this anywhere we'd otherwise reach for `creds.me!.id` to fail fast with a descriptive error.
 */
export const assertMeId = (creds: AuthenticationCreds): string => {
	const id = creds.me?.id
	if (!id) {
		throw new Boom('Cannot proceed: socket is not authenticated yet (creds.me.id is missing)', { statusCode: 401 })
	}

	return id
}

export const initAuthCreds = (): AuthenticationCreds => {
	const identityKey = Curve.generateKeyPair()
	return {
		noiseKey: Curve.generateKeyPair(),
		pairingEphemeralKeyPair: Curve.generateKeyPair(),
		signedIdentityKey: identityKey,
		signedPreKey: signedKeyPair(identityKey, 1),
		registrationId: generateRegistrationId(),
		advSecretKey: randomBytes(32).toString('base64'),
		processedHistoryMessages: [],
		nextPreKeyId: 1,
		firstUnuploadedPreKeyId: 1,
		accountSyncCounter: 0,
		accountSettings: {
			unarchiveChats: false
		},
		registered: false,
		pairingCode: undefined,
		lastPropHash: undefined,
		routingInfo: undefined,
		additionalData: undefined
	}
}
