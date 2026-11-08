import type { SignalDataSet, SignalDataTypeMap, SignalKeyStore } from '../../Types'

export function makeSignalStoreFixture() {
	const data: SignalDataSet = {}
	const setCalls: SignalDataSet[] = []
	const store: SignalKeyStore = {
		async get(type, ids) {
			const out: Record<string, SignalDataTypeMap[typeof type]> = {}
			for (const id of ids) {
				const value = data[type]?.[id]
				if (value !== null && value !== undefined) out[id] = value
			}

			return out
		},
		async set(changes) {
			setCalls.push(structuredClone(changes))
			for (const type of Object.keys(changes) as Array<keyof SignalDataTypeMap>) {
				Object.assign(data, { [type]: { ...data[type], ...changes[type] } })
			}
		}
	}
	return { store, data, setCalls }
}
