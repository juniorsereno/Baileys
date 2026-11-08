import { afterEach, expect, it, jest } from '@jest/globals'
import { mkdtemp, rm } from 'fs/promises'
// @ts-ignore libsignal does not publish types for its protobuf module.
import { PreKeyWhisperMessage } from 'libsignal/src/protobufs'
import { tmpdir } from 'os'
import { join } from 'path'
import { makeLibSignalRepository } from '../../Signal/libsignal'
import { addTransactionCapability } from '../../Utils/auth-utils'
import { Curve, generateSignalPubKey } from '../../Utils/crypto'
import baseLogger from '../../Utils/logger'
import { useMultiFileAuthState } from '../../Utils/use-multi-file-auth-state'

const logger = baseLogger.child({}, { level: 'silent' })
const cleanup: Array<() => Promise<void> | void> = []
afterEach(async () => {
	for (const close of cleanup.splice(0).reverse()) await close()
	jest.restoreAllMocks()
})

async function peer(jid: string) {
	const folder = await mkdtemp(join(tmpdir(), 'baileys-signal-fixture-'))
	cleanup.push(() => rm(folder, { recursive: true, force: true }))
	const { state } = await useMultiFileAuthState(folder)
	const keys = addTransactionCapability(state.keys, logger, { maxCommitRetries: 1, delayBetweenTriesMs: 0 })
	const repo = makeLibSignalRepository({ creds: state.creds, keys }, logger)
	cleanup.push(() => repo.close?.())
	const preKey = Curve.generateKeyPair()
	await keys.set({ 'pre-key': { '1': preKey } })
	return {
		jid,
		keys,
		repo,
		bundle: {
			registrationId: state.creds.registrationId,
			identityKey: generateSignalPubKey(state.creds.signedIdentityKey.public),
			signedPreKey: {
				keyId: state.creds.signedPreKey.keyId,
				publicKey: generateSignalPubKey(state.creds.signedPreKey.keyPair.public),
				signature: state.creds.signedPreKey.signature
			},
			preKey: { keyId: 1, publicKey: generateSignalPubKey(preKey.public) }
		}
	}
}

it('decrypts every message encrypted concurrently through PN and LID in an outer send transaction', async () => {
	const alice = await peer('12025550101@s.whatsapp.net')
	const bob = await peer('12025550102@s.whatsapp.net')
	const lid = '100000000002@lid'
	await alice.repo.lidMapping.storeLIDPNMappings([{ pn: bob.jid, lid }])
	await alice.repo.injectE2ESession({ jid: bob.jid, session: bob.bundle })
	const payloads = Array.from({ length: 12 }, (_, i) => Buffer.from(`fixture-${i}`))
	const encrypted = await alice.keys.transaction(
		() => Promise.all(payloads.map((data, i) => alice.repo.encryptMessage({ jid: i % 2 ? lid : bob.jid, data }))),
		'outbound'
	)
	const received = []
	for (const message of encrypted) {
		received.push(await bob.repo.decryptMessage({ jid: alice.jid, type: message.type, ciphertext: message.ciphertext }))
	}

	expect(received).toEqual(payloads)
})

it('pins one PN/LID resolution across a complete cryptographic operation', async () => {
	const alice = await peer('12025550101@s.whatsapp.net')
	const bob = await peer('12025550102@s.whatsapp.net')
	const lid = '100000000002@lid'
	await alice.repo.lidMapping.storeLIDPNMappings([{ pn: bob.jid, lid }])
	await alice.repo.injectE2ESession({ jid: bob.jid, session: bob.bundle })
	const resolve = jest.spyOn(alice.repo.lidMapping, 'getLIDForPN')
	const encrypted = await alice.repo.encryptMessage({ jid: bob.jid, data: Buffer.from('pinned') })
	expect(resolve).toHaveBeenCalledTimes(1)
	expect(await bob.repo.decryptMessage({ jid: alice.jid, ...encrypted })).toEqual(Buffer.from('pinned'))
})

it('rolls back identity changes when a prekey message fails authentication', async () => {
	const alice = await peer('12025550101@s.whatsapp.net')
	const bob = await peer('12025550102@s.whatsapp.net')
	await alice.repo.injectE2ESession({ jid: bob.jid, session: bob.bundle })
	const encrypted = await alice.repo.encryptMessage({ jid: bob.jid, data: Buffer.from('original') })
	const decoded = PreKeyWhisperMessage.decode(encrypted.ciphertext.subarray(1))
	const message = Buffer.from(decoded.message)
	decoded.message = message
	message[message.length - 1] = message[message.length - 1]! ^ 1
	const corrupted = Buffer.concat([encrypted.ciphertext.subarray(0, 1), PreKeyWhisperMessage.encode(decoded).finish()])
	await expect(
		bob.repo.decryptMessage({ jid: alice.jid, type: encrypted.type, ciphertext: corrupted })
	).rejects.toThrow()
	const address = bob.repo.jidToSignalProtocolAddress(alice.jid)
	expect((await bob.keys.get('identity-key', [address]))[address]).toBeNull()
	expect(await bob.repo.decryptMessage({ jid: alice.jid, ...encrypted })).toEqual(Buffer.from('original'))
})

it('migrates the committed PN session after an in-flight send and continues decrypting through LID', async () => {
	const alice = await peer('12025550101@s.whatsapp.net')
	const bob = await peer('12025550102@s.whatsapp.net')
	const lid = '100000000002@lid'
	await alice.repo.injectE2ESession({ jid: bob.jid, session: bob.bundle })
	await alice.keys.set({ 'device-list': { '12025550102': ['0'] } })
	let encrypted!: Awaited<ReturnType<typeof alice.repo.encryptMessage>>
	let signal!: () => void
	let release!: () => void
	const ready = new Promise<void>(r => {
		signal = r
	})
	const finish = new Promise<void>(r => {
		release = r
	})
	const sending = alice.keys.transaction(async () => {
		encrypted = await alice.repo.encryptMessage({ jid: bob.jid, data: Buffer.from('before migration') })
		signal()
		await finish
	}, 'outbound')
	await ready
	const migrating = alice.repo.migrateSession(bob.jid, lid)
	await new Promise<void>(r => setImmediate(r))
	release()
	await sending
	expect(await migrating).toMatchObject({ migrated: 1 })
	await alice.repo.lidMapping.storeLIDPNMappings([{ pn: bob.jid, lid }])
	expect(await bob.repo.decryptMessage({ jid: alice.jid, ...encrypted })).toEqual(Buffer.from('before migration'))
	const after = await alice.repo.encryptMessage({ jid: lid, data: Buffer.from('after migration') })
	expect(await bob.repo.decryptMessage({ jid: alice.jid, ...after })).toEqual(Buffer.from('after migration'))
})
