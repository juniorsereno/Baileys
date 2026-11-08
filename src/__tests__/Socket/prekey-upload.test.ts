import { jest } from '@jest/globals'
import { EventEmitter } from 'events'
import { mkdtemp, rm } from 'fs/promises'
import { tmpdir } from 'os'
import { join } from 'path'
import P from 'pino'
import { DEFAULT_CONNECTION_CONFIG } from '../../Defaults'
import { useMultiFileAuthState } from '../../Utils/use-multi-file-auth-state'
import { type BinaryNode, decodeBinaryNode, getBinaryNodeChild, getBinaryNodeChildren } from '../../WABinary'

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
const { makeSocket } = await import('../../Socket/socket')
const cleanup: (() => Promise<void>)[] = []
async function createSocket(queryTimeout = 100) {
	const dir = await mkdtemp(join(tmpdir(), 'baileys-prekey-upload-'))
	const { state } = await useMultiFileAuthState(dir)
	const sock = makeSocket({
		...DEFAULT_CONNECTION_CONFIG,
		logger: P({ level: 'silent' }),
		auth: state,
		fireInitQueries: false,
		defaultQueryTimeoutMs: queryTimeout,
		connectTimeoutMs: 100,
		transactionOpts: { maxCommitRetries: 1, delayBetweenTriesMs: 1 }
	})
	cleanup.push(async () => {
		await sock.end(undefined)
		await rm(dir, { recursive: true, force: true })
	})
	if (!(sock.ws instanceof TestTransport)) throw new Error('Expected synthetic transport')
	return { sock, transport: sock.ws, state }
}

function keyIds(node: BinaryNode) {
	return getBinaryNodeChildren(getBinaryNodeChild(node, 'list'), 'key').map(key => {
		const id = getBinaryNodeChild(key, 'id')?.content
		if (!(id instanceof Uint8Array)) throw new Error('Expected encoded prekey ID')
		return Buffer.from(id).readUIntBE(0, id.length)
	})
}

afterEach(async () => {
	for (const clear of cleanup.splice(0)) await clear()
	jest.restoreAllMocks()
	jest.useRealTimers()
})
jest.setTimeout(20000)

test('keeps the upload cursor unchanged until the server acknowledges persisted keys', async () => {
	const { sock, transport, state } = await createSocket()
	transport.hold = true
	const sent = new Promise<void>(resolve => {
		transport.onUpload = resolve
	})
	const upload = sock.uploadPreKeys(3)
	await sent
	const cursorBeforeAck = state.creds.firstUnuploadedPreKeyId
	const allocatedBeforeAck = state.creds.nextPreKeyId
	const node = transport.uploads[0]!
	transport.ack(node)
	await upload
	expect(cursorBeforeAck).toBe(1)
	expect(allocatedBeforeAck).toBe(4)
	expect(keyIds(node)).toEqual([1, 2, 3])
	expect(Object.keys(await state.keys.get('pre-key', ['1', '2', '3']))).toHaveLength(3)
	expect(state.creds.firstUnuploadedPreKeyId).toBe(4)
})

test('reuses the same keys after transient network failure and coalesces callers', async () => {
	const { sock, transport, state } = await createSocket()
	transport.failures = 1
	await Promise.all([sock.uploadPreKeys(3), sock.uploadPreKeys(3)])
	expect(transport.uploads.map(keyIds)).toEqual([
		[1, 2, 3],
		[1, 2, 3]
	])
	expect(getBinaryNodeChild(transport.uploads[0], 'list')).toEqual(getBinaryNodeChild(transport.uploads[1], 'list'))
	expect(state.creds.nextPreKeyId).toBe(4)
	expect(state.creds.firstUnuploadedPreKeyId).toBe(4)
})

test('retains unacknowledged keys after exhausted retries and recovers on a later call', async () => {
	const { sock, transport, state } = await createSocket()
	transport.failures = 4
	await expect(sock.uploadPreKeys(3)).rejects.toThrow('synthetic upload failure')
	expect(state.creds.firstUnuploadedPreKeyId).toBe(1)
	expect(state.creds.nextPreKeyId).toBe(4)
	transport.failures = 0
	await sock.uploadPreKeys(3)
	expect(transport.uploads.map(keyIds)).toEqual(Array.from({ length: 5 }, () => [1, 2, 3]))
	expect(state.creds.firstUnuploadedPreKeyId).toBe(4)
})

test('does not treat a missing IQ response as confirmation', async () => {
	const { sock, transport, state } = await createSocket()
	transport.hold = true
	await expect(sock.uploadPreKeys(2)).rejects.toThrow()
	expect(state.creds.firstUnuploadedPreKeyId).toBe(1)
	transport.hold = false
	await sock.uploadPreKeys(2)
	expect(keyIds(transport.uploads.at(-1)!)).toEqual([1, 2])
	expect(state.creds.firstUnuploadedPreKeyId).toBe(3)
})

test('does not publish credential progress or send keys when persistence fails', async () => {
	const { sock, transport, state } = await createSocket()
	jest.spyOn(state.keys, 'set').mockImplementation(async () => {
		throw new Error('synthetic storage failure')
	})
	await expect(sock.uploadPreKeys(2)).rejects.toThrow('synthetic storage failure')
	expect(state.creds.nextPreKeyId).toBe(1)
	expect(state.creds.firstUnuploadedPreKeyId).toBe(1)
	expect(transport.uploads).toHaveLength(0)
})

test('does not retry a closed socket or acknowledge its pending keys', async () => {
	const { sock, transport, state } = await createSocket()
	transport.failures = 1
	transport.onUpload = () => {
		transport.isOpen = false
	}

	await expect(sock.uploadPreKeys(2)).rejects.toThrow()
	expect(transport.uploads).toHaveLength(1)
	expect(state.creds.firstUnuploadedPreKeyId).toBe(1)
})

test('a late response after the upload deadline cannot advance credentials', async () => {
	const { sock, transport, state } = await createSocket(60000)
	jest.useFakeTimers({ doNotFake: ['nextTick', 'setImmediate'] })
	transport.hold = true
	const sent = new Promise<void>(resolve => {
		transport.onUpload = resolve
	})
	const upload = expect(sock.uploadPreKeys(2)).rejects.toThrow()
	await sent
	await jest.advanceTimersByTimeAsync(30001)
	await upload
	transport.ack(transport.uploads[0]!)
	await jest.advanceTimersByTimeAsync(0)
	expect(state.creds.firstUnuploadedPreKeyId).toBe(1)
	expect(transport.uploads).toHaveLength(1)
	transport.hold = false
	await sock.uploadPreKeys(2)
	expect(state.creds.firstUnuploadedPreKeyId).toBe(3)
	expect(keyIds(transport.uploads[1]!)).toEqual([1, 2])
})
