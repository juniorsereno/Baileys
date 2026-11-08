import { Mutex } from 'async-mutex'

export type LockRef = { namespace: string; id: string }
export const lockRefKey = (ref: LockRef): string => JSON.stringify([ref.namespace, ref.id])

type Entry = { mutex: Mutex; owner?: object; users: number }

/** Record locks can be retained through the outer transaction's durable commit. */
export const makeLockManager = () => {
	const entries = new Map<string, Entry>()
	const waiting = new Map<object, Set<Entry>>()

	const reaches = (owner: object, target: object, seen = new Set<object>()): boolean => {
		if (owner === target) return true
		if (seen.has(owner)) return false
		seen.add(owner)
		for (const entry of waiting.get(owner) ?? []) {
			if (entry.owner && reaches(entry.owner, target, seen)) return true
		}

		return false
	}

	const acquire = async (ref: LockRef, owner: object): Promise<() => void> => {
		const key = lockRefKey(ref)
		let entry = entries.get(key)
		if (!entry) {
			entry = { mutex: new Mutex(), users: 0 }
			entries.set(key, entry)
		}

		if (entry.owner && reaches(entry.owner, owner)) {
			throw new Error('Conflicting nested transaction lock order')
		}

		entry.users++
		const pending = waiting.get(owner) ?? new Set<Entry>()
		waiting.set(owner, pending)
		pending.add(entry)
		const unlock = await entry.mutex.acquire()
		pending.delete(entry)
		if (!pending.size) waiting.delete(owner)
		entry.owner = owner
		let released = false
		return () => {
			if (released) return
			released = true
			entry.owner = undefined
			entry.users--
			unlock()
			if (!entry.users && entries.get(key) === entry) entries.delete(key)
		}
	}

	const withLocks = async <T>(refs: readonly LockRef[], work: () => Promise<T> | T, owner: object = {}): Promise<T> => {
		const releases: Array<() => void> = []
		const unique = new Map(refs.map(ref => [lockRefKey(ref), ref]))
		try {
			for (const [, ref] of [...unique].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
				releases.push(await acquire(ref, owner))
			}

			return await work()
		} finally {
			for (const release of releases.reverse()) release()
		}
	}

	return {
		acquire,
		withLocks,
		withLock: <T>(ref: LockRef, work: () => Promise<T> | T) => withLocks([ref], work),
		isLocked: (ref: LockRef) => !!entries.get(lockRefKey(ref))?.owner
	}
}

export type LockManager = ReturnType<typeof makeLockManager>
