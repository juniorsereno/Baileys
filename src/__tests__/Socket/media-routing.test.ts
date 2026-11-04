import { jest } from '@jest/globals'
import { EventEmitter } from 'events'
import P from 'pino'
import { proto } from '../../../WAProto/index.js'
import { DEFAULT_CONNECTION_CONFIG } from '../../Defaults'
import { initAuthCreds } from '../../Utils/auth-utils'
import { type BinaryNode, decodeBinaryNode } from '../../WABinary'

class TestTransport extends EventEmitter {
	isOpen = true
	isClosed = false
	isClosing = false
	isConnecting = false
	host = 'first.example'
	ttl = '60'
	fail = false
	requests = 0
	connect() {}
	async close() {
		this.isOpen = false
		this.isClosed = true
	}
	send(data: Uint8Array, callback: (err?: Error) => void) {
		const frame = Buffer.from(data)
		void decodeBinaryNode(frame.subarray(frame[0] === 87 ? 7 : 3))
			.then(node => {
				const media = node.attrs.xmlns === 'w:m'
				if (media) this.requests++
				if (media && this.fail) {
					callback(new Error('synthetic IQ failure'))
					return
				}

				const content: BinaryNode[] = media
					? [
							{
								tag: 'media_conn',
								attrs: { auth: 'synthetic', ttl: this.ttl },
								content: [{ tag: 'host', attrs: { hostname: this.host, maxContentLengthBytes: '1000' } }]
							}
						]
					: [
							{ tag: 'count', attrs: { value: '1000' } },
							{ tag: 'digest', attrs: {} }
						]
				this.emit(`TAG:${node.attrs.id}`, { tag: 'iq', attrs: { type: 'result' }, content })
				callback()
			})
			.catch(callback)
		return true
	}
}

jest.unstable_mockModule('../../Socket/Client', () => ({ WebSocketClient: TestTransport }))
const { makeMessagesSocket } = await import('../../Socket/messages-send')
const sockets: ReturnType<typeof makeMessagesSocket>[] = []
function createSocket() {
	const sock = makeMessagesSocket({
		...DEFAULT_CONNECTION_CONFIG,
		logger: P({ level: 'silent' }),
		auth: { creds: initAuthCreds(), keys: { get: async () => ({}), set: async () => {} } },
		fireInitQueries: false,
		defaultQueryTimeoutMs: 100,
		connectTimeoutMs: 100
	})
	sockets.push(sock)
	if (!(sock.ws instanceof TestTransport)) throw new Error('Expected synthetic transport')
	return { sock, transport: sock.ws }
}

afterEach(async () => {
	for (const sock of sockets.splice(0)) await sock.end(undefined)
	jest.restoreAllMocks()
})

test('concurrent first refresh shares one IQ and caches until expiry', async () => {
	const { sock, transport } = createSocket()
	await Promise.all(Array.from({ length: 8 }, () => sock.refreshMediaConn()))
	expect(transport.requests).toBe(1)
	expect(sock.getMediaHost()).toBe('first.example')
	transport.host = 'second.example'
	await sock.refreshMediaConn()
	expect(transport.requests).toBe(1)
	const current = await sock.refreshMediaConn()
	current.fetchDate = new Date(Date.now() - 61000)
	await sock.refreshMediaConn()
	expect(transport.requests).toBe(2)
	expect(sock.getMediaUrl('/item')).toBe('https://second.example/item')
})

test('a failed IQ does not poison subsequent refreshes', async () => {
	const { sock, transport } = createSocket()
	transport.fail = true
	await expect(sock.refreshMediaConn()).rejects.toThrow('synthetic IQ failure')
	transport.fail = false
	await sock.refreshMediaConn()
	expect(sock.getMediaHost()).toBe('first.example')
	expect(transport.requests).toBe(2)
})

test('media hosts remain isolated between sockets and change after forced refresh', async () => {
	const first = createSocket()
	const second = createSocket()
	second.transport.host = 'isolated.example'
	await Promise.all([first.sock.refreshMediaConn(), second.sock.refreshMediaConn()])
	first.transport.host = 'renewed.example'
	await first.sock.refreshMediaConn(true)
	expect(first.sock.getMediaHost()).toBe('renewed.example')
	expect(second.sock.getMediaHost()).toBe('isolated.example')
})

test('login fetches a host before a receive-only socket reports open', async () => {
	const { sock, transport } = createSocket()
	const opened = new Promise<void>(resolve =>
		sock.ev.on('connection.update', update => {
			if (update.connection === 'open') resolve()
		})
	)
	transport.emit('CB:success', { tag: 'success', attrs: {} })
	await opened
	expect(transport.requests).toBe(1)
	expect(sock.getMediaHost()).toBe('first.example')
})

test('socket downloads use the current host and preserve explicit overrides', async () => {
	const { sock, transport } = createSocket()
	await sock.refreshMediaConn()
	const fetchMock = jest.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('synthetic HTTP stop'))
	const media = { mediaKey: Buffer.alloc(32), directPath: '/image', url: 'https://original.example/image' }
	await expect(sock.downloadContentFromMessage(media, 'image')).rejects.toThrow('synthetic HTTP stop')
	expect(fetchMock.mock.calls.at(-1)?.[0]).toBe('https://first.example/image')
	transport.host = 'renewed.example'
	await sock.refreshMediaConn(true)
	await expect(sock.downloadMediaMessage({ key: {}, message: { imageMessage: media } }, 'buffer', {})).rejects.toThrow(
		'synthetic HTTP stop'
	)
	expect(fetchMock.mock.calls.at(-1)?.[0]).toBe('https://renewed.example/image')
	await expect(sock.downloadContentFromMessage(media, 'image', { host: 'override.example' })).rejects.toThrow(
		'synthetic HTTP stop'
	)
	expect(fetchMock.mock.calls.at(-1)?.[0]).toBe('https://override.example/image')
})

test('failed prefetch does not prevent login or a later successful refresh', async () => {
	const { sock, transport } = createSocket()
	transport.fail = true
	const opened = new Promise<void>(resolve =>
		sock.ev.on('connection.update', update => {
			if (update.connection === 'open') resolve()
		})
	)
	transport.emit('CB:success', { tag: 'success', attrs: {} })
	await opened
	expect(sock.getMediaHost()).toBe('mmg.whatsapp.net')
	transport.fail = false
	await sock.refreshMediaConn()
	expect(sock.getMediaHost()).toBe('first.example')
})

test('history notification processing forwards the receiving socket host', async () => {
	const { sock } = createSocket()
	await sock.refreshMediaConn()
	await new Promise<void>(resolve => setImmediate(resolve))
	sock.authState.creds.me = { id: '123@s.whatsapp.net', name: 'Synthetic' }
	const fetchMock = jest.spyOn(globalThis, 'fetch').mockRejectedValue(new Error('synthetic history HTTP stop'))
	await expect(
		sock.upsertMessage(
			{
				key: { id: 'history-host', fromMe: true, remoteJid: '123@s.whatsapp.net' },
				messageTimestamp: 1700000000,
				message: {
					protocolMessage: {
						type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
						historySyncNotification: {
							syncType: proto.HistorySync.HistorySyncType.RECENT,
							mediaKey: Buffer.alloc(32),
							directPath: '/history'
						}
					}
				}
			},
			'notify'
		)
	).rejects.toThrow('synthetic history HTTP stop')
	expect(fetchMock.mock.calls[0]?.[0]).toBe('https://first.example/history')
})
