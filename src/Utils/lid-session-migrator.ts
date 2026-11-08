import type { SignalRepositoryWithLIDStore } from '../Types/Signal'
import { jidNormalizedUser } from '../WABinary'
import { makeKeyedMutex } from './make-mutex'

type MigrationRepository = {
	lidMapping: Pick<SignalRepositoryWithLIDStore['lidMapping'], 'storeLIDPNMappings'>
	migrateSession: SignalRepositoryWithLIDStore['migrateSession']
}

/** Both address orientations share the same lock; persisted sessions decide whether migration is still needed. */
export function makeLidSessionMigrator(repository: MigrationRepository) {
	const locks = makeKeyedMutex()
	return async (pn: string, lid: string): Promise<void> =>
		locks.mutex(jidNormalizedUser(pn), async () => {
			await repository.lidMapping.storeLIDPNMappings([{ pn, lid }])
			// A mapping may survive a failed session write. Its existence cannot certify migration completion.
			await repository.migrateSession(pn, lid)
		})
}
