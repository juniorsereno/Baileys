import NodeCache from '@cacheable/node-cache'
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

				if (!this.hold && node.attrs.xmlns !== 'privacy') this.ack(node)
				callback()
			})
			.catch(callback)
		return true
	}
}
jest.unstable_mockModule('../../Socket/Client', () => ({ WebSocketClient: TestTransport }))
const { makeMessagesSocket } = await import('../../Socket/messages-send')

import { makeLibSignalRepository } from '../../Signal/libsignal'

it.each([false, true])(
	'persists a delayed privacy-token response after the send transaction has committed (nested=%s)',
	async nested => {
		const dir = await mkdtemp(join(tmpdir(), 'baileys-token-fixture-'))
		const { state } = await useMultiFileAuthState(dir)
		state.creds.me = { id: '12025550101:1@s.whatsapp.net' }
		const devices = new NodeCache({ checkperiod: 0, useClones: false })
		for (const user of ['12025550101', '12025550102'])
			devices.set(user, [{ user, device: 1, server: 's.whatsapp.net' }])
		const sock = makeMessagesSocket({
			...DEFAULT_CONNECTION_CONFIG,
			auth: state,
			logger: P({ level: 'silent' }),
			fireInitQueries: false,
			connectTimeoutMs: 1000,
			defaultQueryTimeoutMs: 1000,
			userDevicesCache: {
				get: <T>(key: string) => devices.get(key) as T | undefined,
				set: (key, value) => {
					devices.set(key, value)
				},
				del: key => {
					devices.del(key)
				},
				flushAll: () => devices.flushAll()
			},
			makeSignalRepository: (...args) => ({
				...makeLibSignalRepository(...args),
				validateSession: async () => ({ exists: true }),
				encryptMessage: async () => ({ type: 'msg', ciphertext: Buffer.from('synthetic-encryption') })
			})
		})
		const ws = sock.ws as unknown as TestTransport
		try {
			const send = async () =>
				sock.relayMessage(
					'12025550102@s.whatsapp.net',
					{ conversation: 'synthetic' },
					{ messageId: 'synthetic-token-send' }
				)
			if (nested) await sock.authState.keys.transaction(send, 'parent-send')
			else await send()
			for (let i = 0; i < 100 && !ws.uploads.some(n => n.attrs.xmlns === 'privacy'); i++)
				await new Promise(r => setTimeout(r, 5))
			const iq = ws.uploads.find(n => n.attrs.xmlns === 'privacy')
			expect(iq).toBeDefined()
			await sock.relayMessage(
				'12025550102@s.whatsapp.net',
				{ conversation: 'synthetic second' },
				{ messageId: 'synthetic-second' }
			)
			expect(ws.uploads.filter(n => n.attrs.xmlns === 'privacy')).toHaveLength(1)
			ws.ack(iq!)
			let timestamp: number | undefined
			for (let i = 0; i < 100; i++) {
				timestamp = (await state.keys.get('tctoken', ['12025550102@s.whatsapp.net']))['12025550102@s.whatsapp.net']
					?.senderTimestamp
				if (timestamp) break
				await new Promise(r => setTimeout(r, 5))
			}

			expect(typeof timestamp).toBe('number')
		} finally {
			await sock.end(undefined)
			devices.close()
			await rm(dir, { recursive: true, force: true })
		}
	}
)
