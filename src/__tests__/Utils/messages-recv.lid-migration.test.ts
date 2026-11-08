import { makeLidSessionMigrator } from '../../Utils/lid-session-migrator'

const deferred = () => {
	let resolve!: () => void
	const promise = new Promise<void>(r => {
		resolve = r
	})
	return { promise, resolve }
}

it('serializes mapping and migration for devices of the same PN', async () => {
	const started = deferred(),
		release = deferred()
	const events: string[] = []
	const migrate = makeLidSessionMigrator({
		lidMapping: {
			storeLIDPNMappings: async ([mapping]) => {
				events.push(`map:${mapping!.pn}`)
			}
		},
		migrateSession: async pn => {
			events.push(`start:${pn}`)
			if (pn.includes(':1@')) {
				started.resolve()
				await release.promise
			}

			events.push(`end:${pn}`)
			return { migrated: 1, skipped: 0, total: 1 }
		}
	})
	const first = migrate('12025550101:1@s.whatsapp.net', '100000000001:1@lid')
	await started.promise
	const second = migrate('12025550101:2@s.whatsapp.net', '100000000001:2@lid')
	await new Promise<void>(r => setImmediate(r))
	const premature = [...events]
	release.resolve()
	await Promise.all([first, second])
	expect(premature).toEqual(['map:12025550101:1@s.whatsapp.net', 'start:12025550101:1@s.whatsapp.net'])
	expect(events).toEqual([
		...premature,
		'end:12025550101:1@s.whatsapp.net',
		'map:12025550101:2@s.whatsapp.net',
		'start:12025550101:2@s.whatsapp.net',
		'end:12025550101:2@s.whatsapp.net'
	])
})

it('retries session migration even after its mapping was stored before a failure', async () => {
	let mappings = 0,
		attempts = 0
	const migrate = makeLidSessionMigrator({
		lidMapping: {
			storeLIDPNMappings: async () => {
				mappings++
			}
		},
		migrateSession: async () => {
			if (++attempts === 1) throw new Error('fixture persistence failure')
			return { migrated: 1, skipped: 0, total: 1 }
		}
	})
	await expect(migrate('12025550101@s.whatsapp.net', '100000000001@lid')).rejects.toThrow('fixture persistence failure')
	await migrate('12025550101@s.whatsapp.net', '100000000001@lid')
	expect(mappings).toBe(2)
	expect(attempts).toBe(2)
})

it('allows independent PNs to migrate while another migration is pending', async () => {
	const started = deferred(),
		release = deferred()
	let independentDone = false
	const migrate = makeLidSessionMigrator({
		lidMapping: { storeLIDPNMappings: async () => {} },
		migrateSession: async pn => {
			if (pn === '12025550101@s.whatsapp.net') {
				started.resolve()
				await release.promise
			} else independentDone = true
			return { migrated: 1, skipped: 0, total: 1 }
		}
	})
	const first = migrate('12025550101@s.whatsapp.net', '100000000001@lid')
	await started.promise
	await migrate('12025550102@s.whatsapp.net', '100000000002@lid')
	expect(independentDone).toBe(true)
	release.resolve()
	await first
})
