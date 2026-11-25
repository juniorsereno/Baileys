import { jest } from '@jest/globals'
import { EventEmitter } from 'node:events'
import { setTimeout as sleep } from 'node:timers/promises'
import NodeCache from 'node-cache'
import P from 'pino'
import { DEFAULT_CONNECTION_CONFIG } from '../../Defaults'
import type { CacheStore } from '../../Types'
import { initAuthCreds } from '../../Utils/auth-utils'
import { type BinaryNode, decodeBinaryNode, getBinaryNodeChild, getBinaryNodeChildren, jidDecode } from '../../WABinary'
import { makeSignalStoreFixture } from '../Utils/signal-store-fixture'

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
	recipients: string[][] = []
	deviceLookupStarted = deferred()
	releaseDeviceLookup = deferred()
	deviceQueries = 0
	deviceId = 2
	rejectedPreKeyJids: string[] = []
	preKeyQueries: string[][] = []

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
				if (node.tag === 'message' && id) {
					this.dispatches.push(id)
					const participants = getBinaryNodeChild(node, 'participants')
					this.recipients.push(getBinaryNodeChildren(participants, 'to').map(device => device.attrs.jid!))
				}

				const content: BinaryNode[] = []

				if (node.attrs.xmlns === 'encrypt') {
					const users = getBinaryNodeChildren(getBinaryNodeChild(node, 'key'), 'user')
					const jids = users.map(user => user.attrs.jid!)
					this.preKeyQueries.push(jids)
					if (jids.some(jid => this.rejectedPreKeyJids.includes(jid))) {
						content.push({ tag: 'error', attrs: { code: '406', text: 'not-acceptable' } })
					}
				}

				if (node.attrs.xmlns === 'usync') {
					if (++this.deviceQueries === 1) {
						this.deviceLookupStarted.resolve()
						await this.releaseDeviceLookup.promise
					}

					const request = getBinaryNodeChild(node, 'usync')
					const users = getBinaryNodeChildren(getBinaryNodeChild(request, 'list'), 'user')
					content.push({
						tag: 'usync',
						attrs: {},
						content: [
							{
								tag: 'list',
								attrs: {},
								content: users.map(user => ({
									tag: 'user',
									attrs: user.attrs,
									content: [
										{
											tag: 'devices',
											attrs: {},
											content: [
												{
													tag: 'device-list',
													attrs: {},
													content: [{ tag: 'device', attrs: { id: String(this.deviceId), 'key-index': '1' } }]
												}
											]
										}
									]
								}))
							}
						]
					})
				}

				if (id) this.emit(`TAG:${id}`, { tag: 'iq', attrs: { type: 'result' }, content })
				callback()
			})
			.catch(callback)
		return true
	}
}

jest.unstable_mockModule('../../Socket/Client', () => ({ WebSocketClient: TestTransport }))
const { makeMessagesSocket } = await import('../../Socket/messages-send')

async function createSocket(userDevicesCache?: CacheStore) {
	const creds = initAuthCreds()
	const { store } = makeSignalStoreFixture()
	const socket = makeMessagesSocket({
		...DEFAULT_CONNECTION_CONFIG,
		userDevicesCache,
		logger: P({ level: 'silent' }),
		auth: { creds, keys: store },
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

	return { socket, transport: socket.ws, ownId: creds.me.id, store }
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
			expect(transport.recipients[expectedOrder.indexOf('FIRST')]).toEqual(['10001:2@lid'])
			expect(transport.deviceQueries).toBe(1)
		} finally {
			transport.releaseDeviceLookup.resolve()
			await Promise.allSettled(pending)
			await socket.end(undefined)
		}
	}
)
const phone = '15555550101@s.whatsapp.net'
const lid = '10001@lid'

test.each([
	{ first: lid, second: phone },
	{ first: phone, second: lid }
])('keeps dispatch order for mapped addresses $first and $second', async ({ first, second }) => {
	const { socket, transport, store } = await createSocket()
	const pending = []
	try {
		await store.set({ 'lid-mapping': { '15555550101': '10001', '10001_reverse': '15555550101' } })
		await socket.userDevicesCache.set('15555550100', [])
		const cachedRecipient = jidDecode(second)!
		await socket.userDevicesCache.set(cachedRecipient.user, [{ ...cachedRecipient, device: 0 }])

		pending.push(socket.sendMessage(first, { text: 'First' }, { messageId: 'FIRST' }))
		await transport.deviceLookupStarted.promise
		pending.push(socket.sendMessage(second, { text: 'Second' }, { messageId: 'SECOND' }))
		await Promise.race([pending[1], sleep(150)])
		expect(transport.dispatches).toEqual([])
		transport.releaseDeviceLookup.resolve()
		await Promise.all(pending)

		expect(transport.dispatches).toEqual(['FIRST', 'SECOND'])
	} finally {
		transport.releaseDeviceLookup.resolve()
		await Promise.allSettled(pending)
		await socket.end(undefined)
	}
})

test('a queued send uses the device cache as of the relay transaction', async () => {
	const { socket, transport, ownId } = await createSocket()
	const locked = deferred()
	const release = deferred()
	const pending = [
		socket.authState.keys.transaction(async () => {
			locked.resolve()
			await release.promise
		}, ownId)
	]
	try {
		await locked.promise
		const sent = socket.sendMessage('10002@lid', { text: 'Hello' }, { messageId: 'SEND' })
		await Promise.race([sent, sleep(60)])
		expect(transport.dispatches).toEqual([])
		await socket.userDevicesCache.set('10002', [{ user: '10002', server: 'lid', device: 2 }])
		release.resolve()
		await sent

		expect(transport.recipients).toEqual([['10002:2@lid']])
		expect(transport.deviceQueries).toBe(0)
	} finally {
		release.resolve()
		transport.releaseDeviceLookup.resolve()
		await Promise.allSettled(pending)
		await socket.end(undefined)
	}
})

test('dispatches after one device lookup with the node-cache adapter', async () => {
	const cache: CacheStore = new NodeCache({ stdTTL: 300000, useClones: false })
	const { socket, transport } = await createSocket(cache)
	try {
		transport.releaseDeviceLookup.resolve()
		await socket.sendMessage('10001@lid', { text: 'Hello' }, { messageId: 'SEND' })

		expect(transport.deviceQueries).toBe(1)
		expect(transport.dispatches).toEqual(['SEND'])
		expect(transport.recipients).toEqual([['10001:2@lid']])
	} finally {
		await socket.end(undefined)
	}
})

test.each([
	{ currentDevice: 2, succeeds: true },
	{ currentDevice: 48, succeeds: false }
])(
	'refreshes a rejected device once and dispatches only on recovery ($currentDevice)',
	async ({ currentDevice, succeeds }) => {
		const { socket, transport, store } = await createSocket()
		try {
			await store.set({ 'lid-mapping': { '15555550101': '10001', '10001_reverse': '15555550101' } })
			await socket.userDevicesCache.set('15555550100', [])
			await socket.userDevicesCache.set('15555550101', [{ user: '15555550101', server: 's.whatsapp.net', device: 48 }])
			jest.mocked(socket.signalRepository.validateSession).mockImplementation(async jid => ({
				exists: jidDecode(jid)?.device !== 48
			}))
			transport.deviceId = currentDevice
			transport.rejectedPreKeyJids = ['10001:48@lid']

			const send = socket.sendMessage(phone, { text: 'Hello' }, { messageId: 'RECOVERY' })
			await Promise.race([transport.deviceLookupStarted.promise, send])
			await socket.sendMessage('10002@lid', { text: 'Independent send' }, { messageId: 'OTHER' })
			expect(transport.dispatches).toEqual(['OTHER'])
			transport.releaseDeviceLookup.resolve()

			if (succeeds) {
				await expect(send).resolves.toMatchObject({ key: { id: 'RECOVERY' } })
				expect(transport.dispatches).toEqual(['OTHER', 'RECOVERY'])
				expect(transport.recipients.at(-1)).toEqual(['15555550101:2@s.whatsapp.net'])
			} else {
				await expect(send).rejects.toMatchObject({ message: 'not-acceptable', data: 406 })
				expect(transport.dispatches).toEqual(['OTHER'])
			}

			expect(transport.deviceQueries).toBe(1)
			expect(transport.preKeyQueries).toEqual(succeeds ? [['10001:48@lid']] : [['10001:48@lid'], ['10001:48@lid']])
		} finally {
			transport.releaseDeviceLookup.resolve()
			await socket.end(undefined)
		}
	}
)
