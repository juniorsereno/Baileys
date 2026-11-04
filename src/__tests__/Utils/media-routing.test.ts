import { jest } from '@jest/globals'
import { createCipheriv } from 'crypto'
import { deflateSync } from 'zlib'
import { proto } from '../../../WAProto/index.js'
import { downloadExternalPatch, extractSyncdPatches } from '../../Utils/chat-utils'
import { downloadAndProcessHistorySyncNotification } from '../../Utils/history'
import { downloadContentFromMessage, getMediaKeys } from '../../Utils/messages-media'

const mediaKey = Buffer.alloc(32, 7)
async function serve(plaintext: Uint8Array, type: Parameters<typeof getMediaKeys>[1]) {
	const { cipherKey, iv } = await getMediaKeys(mediaKey, type)
	const cipher = createCipheriv('aes-256-cbc', cipherKey, iv)
	const body = Buffer.concat([cipher.update(plaintext), cipher.final(), Buffer.alloc(10)])
	return jest.spyOn(globalThis, 'fetch').mockImplementation(async () => new Response(body))
}

afterEach(() => jest.restoreAllMocks())

test('history downloads and decodes from the assigned host without changing mappings', async () => {
	const history = proto.HistorySync.encode({
		syncType: proto.HistorySync.HistorySyncType.RECENT,
		conversations: [],
		phoneNumberToLidMappings: [{ lidJid: '123@lid', pnJid: '456@s.whatsapp.net' }]
	}).finish()
	const fetchMock = await serve(deflateSync(history), 'md-msg-hist')
	const result = await downloadAndProcessHistorySyncNotification(
		{ mediaKey, directPath: '/history' },
		{},
		undefined,
		'assigned.example'
	)
	expect(fetchMock.mock.calls[0]?.[0]).toBe('https://assigned.example/history')
	expect(result.lidPnMappings).toEqual([{ lid: '123@lid', pn: '456@s.whatsapp.net' }])
})

test('app-state external snapshots use the assigned host', async () => {
	const fetchMock = await serve(proto.SyncdSnapshot.encode({ version: { version: 3 } }).finish(), 'md-app-state')
	const blob = proto.ExternalBlobReference.encode({ mediaKey, directPath: '/snapshot' }).finish()
	const result = await extractSyncdPatches(
		{
			tag: 'iq',
			attrs: {},
			content: [
				{
					tag: 'sync',
					attrs: {},
					content: [
						{
							tag: 'collection',
							attrs: { name: 'regular' },
							content: [{ tag: 'snapshot', attrs: {}, content: Buffer.from(blob) }]
						}
					]
				}
			]
		},
		{},
		'assigned.example'
	)
	expect(fetchMock.mock.calls[0]?.[0]).toBe('https://assigned.example/snapshot')
	expect(result.regular?.snapshot?.version?.version?.toString()).toBe('3')
})

test('external patches use the assigned host and decode mutations', async () => {
	const fetchMock = await serve(proto.SyncdMutations.encode({ mutations: [] }).finish(), 'md-app-state')
	const result = await downloadExternalPatch({ mediaKey, directPath: '/patch' }, {}, 'assigned.example')
	expect(fetchMock.mock.calls[0]?.[0]).toBe('https://assigned.example/patch')
	expect(result.mutations).toEqual([])
})

test('standalone media utilities retain URL-host fallback and explicit overrides', async () => {
	const fetchMock = await serve(Buffer.from('synthetic media'), 'image')
	const media = { mediaKey, directPath: '/image', url: 'https://original.example/image' }
	const stream = await downloadContentFromMessage(media, 'image')
	const chunks: Buffer[] = []
	for await (const chunk of stream) chunks.push(chunk)
	expect(Buffer.concat(chunks).toString()).toBe('synthetic media')
	expect(fetchMock.mock.calls[0]?.[0]).toBe('https://original.example/image')
	const overridden = await downloadContentFromMessage(media, 'image', { host: 'override.example' })
	for await (const chunk of overridden) expect(chunk.length).toBeGreaterThan(0)
	expect(fetchMock.mock.calls[1]?.[0]).toBe('https://override.example/image')
})
