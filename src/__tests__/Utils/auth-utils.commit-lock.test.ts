import { addTransactionCapability } from '../../Utils/auth-utils'
import baseLogger from '../../Utils/logger'
const logger = baseLogger.child({}, { level: 'silent' })
import type { SignalKeyStore } from '../../Types'

const deferred = () => {
	let resolve!: () => void
	const promise = new Promise<void>(r => {
		resolve = r
	})
	return { promise, resolve }
}

it('retains nested session locks until the outer transaction has persisted', async () => {
	const writes: string[] = []
	const store: SignalKeyStore = {
		get: async () => ({}),
		set: async data => {
			writes.push(Buffer.from(data.session?.peer ?? []).toString())
		}
	}
	const keys = addTransactionCapability(store, logger, { maxCommitRetries: 1, delayBetweenTriesMs: 0 })
	const innerDone = deferred()
	const finishOuter = deferred()
	const outer = keys.transaction(async () => {
		await keys.transactWith({ records: [{ type: 'session', id: 'peer' }] }, async () => {
			await keys.set({ session: { peer: Buffer.from('first') } })
		})
		innerDone.resolve()
		await finishOuter.promise
	}, 'outbound')
	await innerDone.promise
	const competing = keys.transactWith({ records: [{ type: 'session', id: 'peer' }] }, async () => {
		await keys.set({ session: { peer: Buffer.from('second') } })
	})
	await new Promise<void>(r => setImmediate(r))
	const writesBeforeCommit = [...writes]
	finishOuter.resolve()
	await Promise.all([outer, competing])
	expect(writesBeforeCommit).toEqual([])
	expect(writes).toEqual(['first', 'second'])
})

it('aborts conflicting nested lock orders and releases both transactions', async () => {
	const keys = addTransactionCapability({ get: async () => ({}), set: async () => {} }, logger, {
		maxCommitRetries: 1,
		delayBetweenTriesMs: 0
	})
	const first = deferred()
	const second = deferred()
	const results = await Promise.allSettled([
		keys.transactWith({ records: [{ type: 'session', id: 'a' }] }, async () => {
			first.resolve()
			await second.promise
			await keys.transactWith({ records: [{ type: 'session', id: 'b' }] }, async () => {})
		}),
		keys.transactWith({ records: [{ type: 'session', id: 'b' }] }, async () => {
			second.resolve()
			await first.promise
			await keys.transactWith({ records: [{ type: 'session', id: 'a' }] }, async () => {})
		})
	])
	expect(results.filter(r => r.status === 'rejected')).toHaveLength(1)
	expect(results.filter(r => r.status === 'fulfilled')).toHaveLength(1)
	await expect(
		keys.transactWith(
			{
				records: [
					{ type: 'session', id: 'a' },
					{ type: 'session', id: 'b' }
				]
			},
			async () => true
		)
	).resolves.toBe(true)
})

it('serializes sibling operations sharing the same session under one outer transaction', async () => {
	const keys = addTransactionCapability({ get: async () => ({}), set: async () => {} }, logger, {
		maxCommitRetries: 1,
		delayBetweenTriesMs: 0
	})
	let active = 0
	let overlap = false
	await keys.transaction(
		() =>
			Promise.all(
				Array.from({ length: 5 }, () =>
					keys.transactWith({ records: [{ type: 'session', id: 'peer' }] }, async () => {
						active++
						overlap ||= active > 1
						await new Promise<void>(r => setImmediate(r))
						active--
					})
				)
			),
		'outbound'
	)
	expect(overlap).toBe(false)
})

it('releases nested session locks after a durable commit failure', async () => {
	let fail = true
	const writes: string[] = []
	const keys = addTransactionCapability(
		{
			get: async () => ({}),
			set: async data => {
				if (fail) throw new Error('fixture persistence failure')
				writes.push(Buffer.from(data.session?.peer ?? []).toString())
			}
		},
		logger,
		{ maxCommitRetries: 1, delayBetweenTriesMs: 0 }
	)
	await expect(
		keys.transaction(
			() =>
				keys.transactWith({ records: [{ type: 'session', id: 'peer' }] }, async () =>
					keys.set({ session: { peer: Buffer.from('failed') } })
				),
			'outbound'
		)
	).rejects.toThrow('fixture persistence failure')
	fail = false
	await keys.transactWith({ records: [{ type: 'session', id: 'peer' }] }, async () =>
		keys.set({ session: { peer: Buffer.from('saved') } })
	)
	expect(writes).toEqual(['saved'])
})

it('rejects a lock cycle between sibling nested operations instead of hanging the outer transaction', async () => {
	const keys = addTransactionCapability({ get: async () => ({}), set: async () => {} }, logger, {
		maxCommitRetries: 1,
		delayBetweenTriesMs: 0
	})
	const a = deferred()
	const b = deferred()
	await keys.transaction(async () => {
		const results = await Promise.allSettled([
			keys.transactWith({ records: [{ type: 'session', id: 'a' }] }, async () => {
				a.resolve()
				await b.promise
				await keys.transactWith({ records: [{ type: 'session', id: 'b' }] }, async () => {})
			}),
			keys.transactWith({ records: [{ type: 'session', id: 'b' }] }, async () => {
				b.resolve()
				await a.promise
				await keys.transactWith({ records: [{ type: 'session', id: 'a' }] }, async () => {})
			})
		])
		expect(results.filter(r => r.status === 'rejected')).toHaveLength(1)
		expect(results.filter(r => r.status === 'fulfilled')).toHaveLength(1)
	}, 'outbound')
})
