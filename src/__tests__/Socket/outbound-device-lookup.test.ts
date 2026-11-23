import { jest } from '@jest/globals'
import { EventEmitter } from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'
import P from 'pino'
import { DEFAULT_CONNECTION_CONFIG } from '../../Defaults'
import { initAuthCreds } from '../../Utils/auth-utils'
import { decodeBinaryNode } from '../../WABinary'

const deferred = () => {
	let resolve!: () => void
	const promise = new Promise<void>(done => {
		resolve = done
	})
	return { promise, resolve }
}

class TestTransport extends EventEmitter {
	isOpen = true
	isClosed = false
	isClosing = false
	isConnecting = false
	dispatches: string[] = []
	deviceLookupStarted = deferred()
	releaseDeviceLookup = deferred()
	deviceQueries = 0

	connect() {}

	async close() {
		this.isOpen = false
		this.isClosed = true
	}

	send(data: Uint8Array, callback: (err?: Error) => void) {
		const frame = Buffer.from(data)
		void decodeBinaryNode(frame.subarray(frame[0] === 87 ? 7 : 3))
			.then(async node => {
				const id = node.attrs.id
				if (node.tag === 'message' && id) this.dispatches.push(id)
				if (node.attrs.xmlns === 'usync' && ++this.deviceQueries === 1) {
					this.deviceLookupStarted.resolve()
					await this.releaseDeviceLookup.promise
				}

				if (id) this.emit(`TAG:${id}`, { tag: 'iq', attrs: { type: 'result' }, content: [] })
				callback()
			})
			.catch(callback)
		return true
	}
}

jest.unstable_mockModule('../../Socket/Client', () => ({ WebSocketClient: TestTransport }))
const { makeMessagesSocket } = await import('../../Socket/messages-send')

async function createSocket() {
	const creds = initAuthCreds()
	const socket = makeMessagesSocket({
		...DEFAULT_CONNECTION_CONFIG,
		logger: P({ level: 'silent' }),
		auth: { creds, keys: { get: async () => ({}), set: async () => {} } },
		fireInitQueries: false,
		emitOwnEvents: false,
		defaultQueryTimeoutMs: 2000,
		connectTimeoutMs: 2000
	})

	await new Promise<void>(resolve => setImmediate(resolve))
	creds.me = { id: '15555550100:3@s.whatsapp.net', lid: '10000:3@lid' }
	if (!(socket.ws instanceof TestTransport)) throw new Error('Expected a test transport')

	await socket.userDevicesCache.set('10000', [])
	await socket.userDevicesCache.set('10002', [{ user: '10002', server: 'lid', device: 0 }])
	jest.spyOn(socket.signalRepository, 'validateSession').mockResolvedValue({ exists: true })
	jest.spyOn(socket.signalRepository, 'encryptMessage').mockResolvedValue({ type: 'msg', ciphertext: Buffer.from([1]) })

	return { socket, transport: socket.ws }
}

test.each([
	{ recipient: '10002@lid', expectedWhileLookupPending: ['SECOND'], expectedOrder: ['SECOND', 'FIRST'] },
	{ recipient: '10001@lid', expectedWhileLookupPending: [], expectedOrder: ['FIRST', 'SECOND'] }
])(
	'preserves dispatch order for $recipient when a device lookup is pending',
	async ({ recipient, expectedWhileLookupPending, expectedOrder }) => {
		const { socket, transport } = await createSocket()
		const pending = []
		try {
			pending.push(socket.sendMessage('10001@lid', { text: 'First' }, { messageId: 'FIRST' }))
			await transport.deviceLookupStarted.promise

			pending.push(socket.sendMessage(recipient, { text: 'Second' }, { messageId: 'SECOND' }))
			await Promise.race([pending[1], sleep(150)])
			expect(transport.dispatches).toEqual(expectedWhileLookupPending)

			transport.releaseDeviceLookup.resolve()
			await Promise.all(pending)
			expect(transport.dispatches).toEqual(expectedOrder)
		} finally {
			transport.releaseDeviceLookup.resolve()
			await Promise.allSettled(pending)
			await socket.end(undefined)
		}
	}
)
