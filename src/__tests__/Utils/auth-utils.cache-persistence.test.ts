import type { SignalKeyStore } from '../../Types'
import { makeCacheableSignalKeyStore } from '../../Utils/auth-utils'

describe('Signal key cache persistence', () => {
	it('does not expose a key whose first durable write failed', async () => {
		const store: SignalKeyStore = {
			get: async () => ({}),
			set: async () => {
				throw new Error('durable write failed')
			}
		}
		const cached = makeCacheableSignalKeyStore(store)
		await expect(cached.set({ session: { peer: Buffer.from('uncommitted') } })).rejects.toThrow('durable write failed')
		expect(await cached.get('session', ['peer'])).toEqual({})
	})

	it('keeps the last committed key after a failed replacement and accepts a successful retry', async () => {
		let fail = false
		const store: SignalKeyStore = {
			get: async () => ({}),
			set: async () => {
				if (fail) throw new Error('durable write failed')
			}
		}
		const cached = makeCacheableSignalKeyStore(store)
		const oldKey = Buffer.from('committed')
		await cached.set({ session: { peer: oldKey } })
		fail = true
		await expect(cached.set({ session: { peer: Buffer.from('uncommitted') } })).rejects.toThrow('durable write failed')
		expect(await cached.get('session', ['peer'])).toEqual({ peer: oldKey })
		fail = false
		const newKey = Buffer.from('committed retry')
		await cached.set({ session: { peer: newKey } })
		expect(await cached.get('session', ['peer'])).toEqual({ peer: newKey })
	})

	it('keeps the key when a durable deletion fails', async () => {
		let fail = false
		const store: SignalKeyStore = {
			get: async () => ({}),
			set: async () => {
				if (fail) throw new Error('delete failed')
			}
		}
		const cached = makeCacheableSignalKeyStore(store)
		const key = Buffer.from('committed')
		await cached.set({ session: { peer: key } })
		fail = true
		await expect(cached.set({ session: { peer: null } })).rejects.toThrow('delete failed')
		expect(await cached.get('session', ['peer'])).toEqual({ peer: key })
	})
	it('serializes a reader behind an in-flight failed durable write', async () => {
		let fail = false
		let markEntered!: () => void
		let rejectWrite!: (error: Error) => void
		const entered = new Promise<void>(resolve => {
			markEntered = resolve
		})
		const completion = new Promise<void>((_resolve, reject) => {
			rejectWrite = reject
		})
		const store: SignalKeyStore = {
			get: async () => ({}),
			set: async () => {
				if (fail) {
					markEntered()
					await completion
				}
			}
		}
		const cached = makeCacheableSignalKeyStore(store)
		const oldKey = Buffer.from('committed')
		await cached.set({ session: { peer: oldKey } })
		fail = true
		const write = expect(cached.set({ session: { peer: Buffer.from('uncommitted') } })).rejects.toThrow('write failed')
		await entered
		const read = cached.get('session', ['peer'])
		rejectWrite(new Error('write failed'))
		await write
		expect(await read).toEqual({ peer: oldKey })
	})
})
