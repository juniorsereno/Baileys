import { jest } from '@jest/globals'
import { EventEmitter } from 'events'
import { mkdtemp, rm } from 'fs/promises'
import { tmpdir } from 'os'
import { join } from 'path'
import P from 'pino'
import { DEFAULT_CONNECTION_CONFIG } from '../../Defaults'
import { useMultiFileAuthState } from '../../Utils/use-multi-file-auth-state'
import { type BinaryNode, decodeBinaryNode } from '../../WABinary'

class TestTransport extends EventEmitter {
	isOpen = true
	isClosed = false
	isClosing = false
	isConnecting = false
	failures = 0
	sent: BinaryNode[] = []
	onSent: (node: BinaryNode) => void = () => {}
	connect() {}
	async close() {
		this.isOpen = false
		this.isClosed = true
		this.emit('close')
	}
	send(data: Uint8Array, callback: (err?: Error) => void) {
		const frame = Buffer.from(data)
		void decodeBinaryNode(frame.subarray(frame[0] === 87 ? 7 : 3))
			.then(node => {
				this.sent.push(node)
				this.onSent(node)
				callback(this.failures-- > 0 ? new Error('synthetic ack failure') : undefined)
			})
			.catch(callback)
		return true
	}
}

jest.unstable_mockModule('../../Socket/Client', () => ({ WebSocketClient: TestTransport }))
const { makeMessagesRecvSocket } = await import('../../Socket/messages-recv')
const cleanup: (() => Promise<void>)[] = []

async function createSocket(ignoreStatuses = false) {
	const dir = await mkdtemp(join(tmpdir(), 'baileys-status-ack-'))
	const { state } = await useMultiFileAuthState(dir)
	state.creds.me = { id: '12025550101@s.whatsapp.net', name: 'Synthetic' }
	const sock = makeMessagesRecvSocket({
		...DEFAULT_CONNECTION_CONFIG,
		auth: state,
		logger: P({ level: 'silent' }),
		fireInitQueries: false,
		connectTimeoutMs: 100,
		defaultQueryTimeoutMs: 100,
		shouldIgnoreJid: jid => ignoreStatuses && jid === 'status@broadcast'
	})
	cleanup.push(async () => {
		await sock.end(undefined)
		await rm(dir, { recursive: true, force: true })
	})
	if (!(sock.ws instanceof TestTransport)) throw new Error('Expected synthetic transport')
	const upserts = jest.fn()
	sock.ev.on('messages.upsert', upserts)
	return { transport: sock.ws, upserts }
}

afterEach(async () => {
	for (const clear of cleanup.splice(0)) await clear()
})

function status(id: string, type: string, offline: boolean): BinaryNode {
	return {
		tag: 'status',
		attrs: {
			id,
			from: 'status@broadcast',
			type,
			participant: '12025550102@s.whatsapp.net',
			...(offline ? { offline: '3' } : {})
		}
	}
}

async function dispatch(transport: TestTransport, node: BinaryNode) {
	const sent = new Promise<void>(resolve => {
		transport.onSent = ack => {
			if (ack.attrs.id === node.attrs.id) resolve()
		}
	})
	if (!transport.emit('CB:status', node)) return
	await sent
}

test.each([
	['media', false],
	['text', false],
	['media', true],
	['text', true]
])('acknowledges %s statuses (offline=%s) without publishing chat messages', async (type, offline) => {
	const { transport, upserts } = await createSocket()
	const node = status('synthetic-status', type, offline)
	await dispatch(transport, node)
	expect(transport.sent).toEqual([
		{
			tag: 'ack',
			attrs: {
				id: node.attrs.id,
				to: 'status@broadcast',
				class: 'status',
				type,
				participant: node.attrs.participant
			}
		}
	])
	expect(upserts).not.toHaveBeenCalled()
})

test('acknowledges ignored statuses without an error code', async () => {
	const { transport } = await createSocket(true)
	await dispatch(transport, status('ignored-status', 'media', true))
	expect(transport.sent[0]?.attrs).toMatchObject({ class: 'status', type: 'media' })
	expect(transport.sent[0]?.attrs).not.toHaveProperty('error')
})

test.each([false, true])('a failed status ack does not block the next status (ignored=%s)', async ignored => {
	const { transport } = await createSocket(ignored)
	transport.failures = 1
	await dispatch(transport, status('failed-status', 'media', true))
	await dispatch(transport, status('next-status', 'text', true))
	expect(transport.sent.map(node => node.attrs.id)).toEqual(['failed-status', 'next-status'])
})
