import NodeCache from '@cacheable/node-cache'
import { jest } from '@jest/globals'
import { EventEmitter } from 'events'
import { mkdtemp, rm } from 'fs/promises'
import { tmpdir } from 'os'
import { join } from 'path'
import P from 'pino'
import { DEFAULT_CONNECTION_CONFIG } from '../../Defaults'
import type { CacheStore } from '../../Types'
import { useMultiFileAuthState } from '../../Utils/use-multi-file-auth-state'
import { type BinaryNode, decodeBinaryNode, getBinaryNodeChild } from '../../WABinary'

class TestTransport extends EventEmitter {
	isOpen = true
	isClosed = false
	isClosing = false
	isConnecting = false
	failures = 0
	hold = false
	uploads: BinaryNode[] = []
	onUpload: () => void = () => {}
	connect() {}
	async close() {
		this.isOpen = false
		this.isClosed = true
		this.emit('close')
	}
	ack(node: BinaryNode) {
		this.emit(`TAG:${node.attrs.id}`, { tag: 'iq', attrs: { type: 'result', id: node.attrs.id } })
	}
	send(data: Uint8Array, callback: (err?: Error) => void) {
		const frame = Buffer.from(data)
		void decodeBinaryNode(frame.subarray(frame[0] === 87 ? 7 : 3))
			.then(node => {
				this.uploads.push(node)
				this.onUpload()
				if (this.failures-- > 0) {
					callback(new Error('synthetic upload failure'))
					return
				}

				if (!this.hold) this.ack(node)
				callback()
			})
			.catch(callback)
		return true
	}
}
jest.unstable_mockModule('../../Socket/Client', () => ({ WebSocketClient: TestTransport }))
const { makeMessagesSocket } = await import('../../Socket/messages-send')
const relay = jest.fn<ReturnType<typeof makeMessagesSocket>['relayMessage']>().mockResolvedValue('synthetic-relay')
jest.unstable_mockModule('../../Socket/messages-send', () => ({
	makeMessagesSocket: (config: Parameters<typeof makeMessagesSocket>[0]) => ({
		...makeMessagesSocket(config),
		assertSessions: async () => true,
		relayMessage: relay
	})
}))
const { makeMessagesRecvSocket: makeSocket } = await import('../../Socket/messages-recv')
const cleanup: (() => Promise<void>)[] = []
async function createSocket(enableRecentMessageCache: boolean, messageAvailable = false) {
	const dir = await mkdtemp(join(tmpdir(), 'baileys-prekey-upload-'))
	const { state } = await useMultiFileAuthState(dir)
	const cache = new NodeCache<number>({ useClones: false, checkperiod: 0 })
	const counter: CacheStore = {
		get: async <T>(key: string) => {
			const value = cache.get(key)
			if (typeof value !== 'number') throw new Error('Expected seeded retry counter')
			await new Promise<void>(resolve => setImmediate(resolve))
			return value as T
		},
		set: async (key, value) => {
			await new Promise<void>(resolve => setImmediate(resolve))
			if (typeof value !== 'number') throw new Error('Expected retry count')
			cache.set(key, value)
		},
		del: key => cache.del(key),
		flushAll: () => cache.flushAll()
	}
	state.creds.me = { id: '123@s.whatsapp.net', name: 'Synthetic' }
	state.creds.account = {}
	const sock = makeSocket({
		...DEFAULT_CONNECTION_CONFIG,
		logger: P({ level: 'silent' }),
		auth: state,
		fireInitQueries: false,
		enableRecentMessageCache,
		getMessage: async () => (messageAvailable ? { conversation: 'Synthetic retry fixture' } : undefined),
		enableAutoSessionRecreation: false,
		msgRetryCounterCache: counter,
		defaultQueryTimeoutMs: 100,
		connectTimeoutMs: 100,
		transactionOpts: { maxCommitRetries: 1, delayBetweenTriesMs: 1 }
	})
	cleanup.push(async () => {
		await sock.end(undefined)
		await rm(dir, { recursive: true, force: true })
	})
	if (!(sock.ws instanceof TestTransport)) throw new Error('Expected synthetic transport')
	return { sock, transport: sock.ws, state, cache }
}

afterEach(async () => {
	for (const clear of cleanup.splice(0)) await clear()
	jest.restoreAllMocks()
	relay.mockClear()
})
jest.setTimeout(20000)

const node: BinaryNode = {
	tag: 'message',
	attrs: { from: '456@s.whatsapp.net', id: 'retry-example', t: '1700000000', type: 'text' }
}
function receiptCounts(transport: TestTransport) {
	return transport.uploads
		.filter(n => n.tag === 'receipt' && n.attrs.type === 'retry')
		.map(n => Number(getBinaryNodeChild(n, 'retry')?.attrs.count))
}

test.each([false, true])('concurrent retry requests reserve distinct counts (manager=%s)', async enabled => {
	const { sock, transport, cache } = await createSocket(enabled)
	cache.set('retry-example:undefined', 2)
	sock.messageRetryManager?.incrementRetryCount('retry-example')
	sock.messageRetryManager?.incrementRetryCount('retry-example')
	await Promise.all([sock.sendRetryRequest(node), sock.sendRetryRequest(node)])
	expect(receiptCounts(transport).sort()).toEqual([3, 4])
	expect(cache.get('retry-example:undefined')).toBe(4)
})

test.each([false, true])('exhausted retries stay exhausted instead of resetting (manager=%s)', async enabled => {
	const { sock, transport, cache } = await createSocket(enabled)
	cache.set('retry-example:undefined', 5)
	for (let i = 0; i < 5; i++) sock.messageRetryManager?.incrementRetryCount('retry-example')
	await sock.sendRetryRequest(node)
	await sock.sendRetryRequest(node)
	expect(receiptCounts(transport)).toEqual([])
	expect(cache.get('retry-example:undefined')).toBe(5)
})

test.each([false, true])('a failed counter write sends no receipt and can be retried (manager=%s)', async enabled => {
	const { sock, transport, cache } = await createSocket(enabled)
	cache.set('retry-example:undefined', 2)
	sock.messageRetryManager?.incrementRetryCount('retry-example')
	sock.messageRetryManager?.incrementRetryCount('retry-example')
	const spy = jest.spyOn(cache, 'set').mockImplementationOnce(() => {
		throw new Error('counter unavailable')
	})
	await expect(sock.sendRetryRequest(node)).rejects.toThrow('counter unavailable')
	expect(receiptCounts(transport)).toEqual([])
	spy.mockRestore()
	await sock.sendRetryRequest(node)
	expect(receiptCounts(transport)).toEqual([3])
})

test.each([false, true])(
	'concurrent requests cannot exceed the remaining retry allowance (manager=%s)',
	async enabled => {
		const { sock, transport, cache } = await createSocket(enabled)
		cache.set('retry-example:undefined', 4)
		for (let i = 0; i < 4; i++) sock.messageRetryManager?.incrementRetryCount('retry-example')
		await Promise.all(Array.from({ length: 4 }, () => sock.sendRetryRequest(node)))
		expect(receiptCounts(transport)).toEqual([5])
		expect(cache.get('retry-example:undefined')).toBe(5)
	}
)

test('retry counters remain isolated between sockets', async () => {
	const first = await createSocket(false)
	const second = await createSocket(false)
	first.cache.set('retry-example:undefined', 5)
	second.cache.set('retry-example:undefined', 2)
	await Promise.all([first.sock.sendRetryRequest(node), second.sock.sendRetryRequest(node)])
	expect(receiptCounts(first.transport)).toEqual([])
	expect(receiptCounts(second.transport)).toEqual([3])
})

const peer = '456:1@s.whatsapp.net'
function receipt(): BinaryNode {
	return {
		tag: 'receipt',
		attrs: { from: peer, id: 'retry-example', type: 'retry', t: '1700000000' },
		content: [{ tag: 'retry', attrs: { count: '1', id: 'retry-example' } }]
	}
}

function dispatchReceipt(transport: TestTransport) {
	return new Promise<void>(resolve => {
		transport.onUpload = () => {
			if (transport.uploads.at(-1)?.tag === 'ack') resolve()
		}

		transport.emit('CB:receipt', receipt())
	})
}

test.each([false, true])('an outbound resend consumes exactly one attempt (manager=%s)', async enabled => {
	const { transport, cache } = await createSocket(enabled, true)
	cache.set(`retry-example:${peer}`, 2)
	await dispatchReceipt(transport)
	expect(relay).toHaveBeenCalledTimes(1)
	expect(cache.get(`retry-example:${peer}`)).toBe(3)
})

test.each([false, true])('receipt and decryption paths share retry reservations (manager=%s)', async enabled => {
	const { sock, transport, cache } = await createSocket(enabled, true)
	cache.set(`retry-example:${peer}`, 2)
	const inbound = { ...node, attrs: { ...node.attrs, participant: peer } }
	await Promise.all([sock.sendRetryRequest(inbound), dispatchReceipt(transport)])
	expect(relay).toHaveBeenCalledTimes(1)
	expect(cache.get(`retry-example:${peer}`)).toBe(4)
	expect(receiptCounts(transport)).toHaveLength(1)
})

test('a receipt for an unavailable message does not consume a resend attempt', async () => {
	const { transport, cache } = await createSocket(false)
	cache.set(`retry-example:${peer}`, 2)
	await dispatchReceipt(transport)
	expect(relay).not.toHaveBeenCalled()
	expect(cache.get(`retry-example:${peer}`)).toBe(2)
})

test('a retry cannot consume a prekey from an upload awaiting acknowledgement', async () => {
	const { sock, transport, cache } = await createSocket(false)
	cache.set('retry-example:undefined', 2)
	transport.hold = true
	const sent = new Promise<void>(resolve => {
		transport.onUpload = resolve
	})
	const upload = sock.uploadPreKeys(2)
	await sent
	const uploadNode = transport.uploads.find(n => n.tag === 'iq')!
	const transaction = jest.spyOn(sock.authState.keys, 'transaction')
	const set = cache.set.bind(cache)
	const reserved = new Promise<void>(resolve => {
		jest.spyOn(cache, 'set').mockImplementation((key, value, ttl) => {
			const result = set(key, value, ttl)
			resolve()
			return result
		})
	})
	const retry = sock.sendRetryRequest(node)
	await reserved
	await new Promise<void>(resolve => setImmediate(resolve))
	const earlyTransactions = transaction.mock.calls.length
	transport.ack(uploadNode)
	await Promise.all([upload, retry])
	expect(earlyTransactions).toBe(0)
	const receipt = transport.uploads.find(n => n.tag === 'receipt' && n.attrs.type === 'retry')!
	const key = getBinaryNodeChild(getBinaryNodeChild(receipt, 'keys'), 'key')
	const id = getBinaryNodeChild(key, 'id')?.content
	if (!(id instanceof Uint8Array)) throw new Error('Expected retry prekey')
	expect(Buffer.from(id).readUIntBE(0, id.length)).toBe(3)
})
