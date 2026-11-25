import NodeCache from '@cacheable/node-cache'
import { Boom, isBoom } from '@hapi/boom'
import { proto } from '../../WAProto/index.js'
import { DEFAULT_CACHE_TTLS, WA_DEFAULT_EPHEMERAL } from '../Defaults'
import type {
	AnyMessageContent,
	MessageReceiptType,
	MessageRelayOptions,
	MiscMessageGenerationOptions,
	SocketConfig,
	WAMessage,
	WAMessageKey
} from '../Types'
import {
	aggregateMessageKeysNotFromMe,
	assertMediaContent,
	assertMeId,
	bindWaitForEvent,
	decryptMediaRetryData,
	downloadContentFromMessage,
	downloadMediaMessage,
	encodeNewsletterMessage,
	encodeSignedDeviceIdentity,
	encodeWAMessage,
	encryptMediaRetryRequest,
	extractDeviceJids,
	generateMessageIDV2,
	generateParticipantHashV2,
	generateWAMessage,
	getStatusCodeForMediaRetry,
	getUrlFromDirectPath,
	getWAUploadToServer,
	MessageRetryManager,
	normalizeMessageContent,
	parseAndInjectE2ESessions,
	unixTimestampSeconds
} from '../Utils'
import { getUrlInfo } from '../Utils/link-preview'
import { makeKeyedMutex, makeMutex } from '../Utils/make-mutex'
import { getMessageReportingToken, shouldIncludeReportingToken } from '../Utils/reporting-utils'
import {
	buildMergedTcTokenIndexWrite,
	isTcTokenExpired,
	resolveIssuanceJid,
	resolveTcTokenJid,
	shouldSendNewTcToken,
	storeTcTokensFromIqResult
} from '../Utils/tc-token-utils'
import {
	areJidsSameUser,
	type BinaryNode,
	type BinaryNodeAttributes,
	type FullJid,
	isHostedLidUser,
	isHostedPnUser,
	isJidBot,
	isJidGroup,
	isJidMetaAI,
	isJidNewsletter,
	isJidStatusBroadcast,
	isLidUser,
	isPnUser,
	jidDecode,
	jidEncode,
	jidNormalizedUser,
	type JidWithDevice,
	PSA_WID,
	S_WHATSAPP_NET
} from '../WABinary'
import { USyncQuery, USyncUser } from '../WAUSync'
import { makeNewsletterSocket } from './newsletter'

class DeviceSessionQueryRejected extends Error {
	constructor(readonly rejection: Boom) {
		super(rejection.message)
	}
}

export const makeMessagesSocket = (config: SocketConfig) => {
	const {
		logger,
		linkPreviewImageThumbnailWidth,
		generateHighQualityLinkPreview,
		options: httpRequestOptions,
		patchMessageBeforeSending,
		cachedGroupMetadata,
		enableRecentMessageCache,
		maxMsgRetryCount
	} = config
	const sock = makeNewsletterSocket(config)
	const {
		ev,
		authState,
		messageMutex,
		signalRepository,
		upsertMessage,
		query,
		fetchPrivacySettings,
		sendNode,
		groupMetadata,
		groupToggleEphemeral,
		registerSocketEndHandler,
		refreshMediaConn,
		getMediaHost
	} = sock

	const getLIDForPN = signalRepository.lidMapping.getLIDForPN.bind(signalRepository.lidMapping)

	/**
	 * Set of tctoken storage JIDs with a fire-and-forget `issuePrivacyTokens` IQ in flight.
	 * Prevents duplicate IQs from rapid back-to-back sends before `senderTimestamp` persists.
	 * Entries are always removed in `.finally()`, so the set is bounded by concurrency.
	 */
	const inFlightTcTokenIssuance = new Set<string>()

	const userDevicesCache =
		config.userDevicesCache ||
		new NodeCache<JidWithDevice[]>({
			stdTTL: DEFAULT_CACHE_TTLS.USER_DEVICES, // 5 minutes
			useClones: false
		})
	/** Serializes writes to userDevicesCache across USync refresh and device-notification handling. */
	const devicesMutex = makeMutex()

	// Initialize message retry manager if enabled
	const messageRetryManager = enableRecentMessageCache ? new MessageRetryManager(logger, maxMsgRetryCount) : null

	// Prevent race conditions in Signal session encryption by user
	const encryptionMutex = makeKeyedMutex()
	const directRelayIdentityMutex = makeMutex()
	const directRelayMutex = makeKeyedMutex()

	/**
	 * generic send receipt function
	 * used for receipts of phone call, read, delivery etc.
	 * */
	const sendReceipt = async (
		jid: string,
		participant: string | undefined,
		messageIds: string[],
		type: MessageReceiptType
	) => {
		if (!messageIds || messageIds.length === 0) {
			throw new Boom('missing ids in receipt')
		}

		const node: BinaryNode = {
			tag: 'receipt',
			attrs: {
				id: messageIds[0]!
			}
		}
		const isReadReceipt = type === 'read' || type === 'read-self'
		if (isReadReceipt) {
			node.attrs.t = unixTimestampSeconds().toString()
		}

		if (type === 'sender' && (isPnUser(jid) || isLidUser(jid))) {
			node.attrs.recipient = jid
			node.attrs.to = participant!
		} else {
			node.attrs.to = jid
			if (participant) {
				node.attrs.participant = participant
			}
		}

		if (type) {
			node.attrs.type = type
		}

		const remainingMessageIds = messageIds.slice(1)
		if (remainingMessageIds.length) {
			node.content = [
				{
					tag: 'list',
					attrs: {},
					content: remainingMessageIds.map(id => ({
						tag: 'item',
						attrs: { id }
					}))
				}
			]
		}

		logger.debug({ attrs: node.attrs, messageIds }, 'sending receipt for messages')
		await sendNode(node)
	}

	/** Correctly bulk send receipts to multiple chats, participants */
	const sendReceipts = async (keys: WAMessageKey[], type: MessageReceiptType) => {
		const recps = aggregateMessageKeysNotFromMe(keys)
		for (const { jid, participant, messageIds } of recps) {
			await sendReceipt(jid, participant, messageIds, type)
		}
	}

	/** Bulk read messages. Keys can be from different chats & participants */
	const readMessages = async (keys: WAMessageKey[]) => {
		const privacySettings = await fetchPrivacySettings()
		// based on privacy settings, we have to change the read type
		const readType = privacySettings.readreceipts === 'all' ? 'read' : 'read-self'
		await sendReceipts(keys, readType)
	}

	/** Device info with wire JID */
	type DeviceWithJid = JidWithDevice & {
		jid: string
	}

	const collectKnownDevices = async (jids: string[], useCache: boolean) => {
		const deviceResults: DeviceWithJid[] = []
		const toFetch: string[] = []

		const jidsWithUser = jids
			.map(jid => {
				const decoded = jidDecode(jid)
				const user = decoded?.user
				const device = decoded?.device
				const isExplicitDevice = typeof device === 'number' && device >= 0

				if (isExplicitDevice && user) {
					deviceResults.push({
						user,
						device,
						jid
					})
					return null
				}

				jid = jidNormalizedUser(jid)
				return { jid, user }
			})
			.filter(jid => jid !== null)

		let mgetDevices: undefined | Record<string, FullJid[] | undefined>

		if (useCache && userDevicesCache.mget) {
			const usersToFetch = jidsWithUser.map(j => j?.user).filter(Boolean) as string[]
			mgetDevices = await userDevicesCache.mget(usersToFetch)
		}

		for (const { jid, user } of jidsWithUser) {
			if (useCache) {
				const devices =
					mgetDevices?.[user!] ||
					(userDevicesCache.mget ? undefined : ((await userDevicesCache.get(user!)) as FullJid[]))
				if (devices) {
					const devicesWithJid = devices.map(d => ({
						...d,
						jid: jidEncode(d.user, d.server, d.device)
					}))
					deviceResults.push(...devicesWithJid)

					logger.trace({ user }, 'using cache for devices')
				} else {
					toFetch.push(jid)
				}
			} else {
				toFetch.push(jid)
			}
		}

		return { deviceResults, toFetch }
	}

	const fetchUSyncDevices = async (toFetch: string[], ignoreZeroDevices: boolean): Promise<DeviceWithJid[]> => {
		const deviceResults: DeviceWithJid[] = []
		if (!toFetch.length) return deviceResults

		const requestedLidUsers = new Set<string>()
		for (const jid of toFetch) {
			if (isLidUser(jid) || isHostedLidUser(jid)) {
				const user = jidDecode(jid)?.user
				if (user) requestedLidUsers.add(user)
			}
		}

		const query = new USyncQuery().withContext('message').withDeviceProtocol().withLIDProtocol()

		for (const jid of toFetch) {
			query.withUser(new USyncUser().withId(jid)) // todo: investigate - the idea here is that <user> should have an inline lid field with the lid being the pn equivalent
		}

		const result = await sock.executeUSyncQuery(query)

		if (result) {
			// TODO: LID MAP this stuff (lid protocol will now return lid with devices)
			const lidResults = result.list.filter(a => !!a.lid)
			if (lidResults.length > 0) {
				logger.trace('Storing LID maps from device call')
				await signalRepository.lidMapping.storeLIDPNMappings(lidResults.map(a => ({ lid: a.lid as string, pn: a.id })))

				// Force-refresh sessions for newly mapped LIDs to align identity addressing
				try {
					const lids = lidResults.map(a => a.lid as string)
					if (lids.length) {
						await assertSessions(lids, true)
					}
				} catch (e) {
					logger.warn({ e, count: lidResults.length }, 'failed to assert sessions for newly mapped LIDs')
				}
			}

			const extracted = extractDeviceJids(
				result?.list,
				authState.creds.me!.id,
				authState.creds.me!.lid!,
				ignoreZeroDevices
			)
			const deviceMap: { [_: string]: FullJid[] } = {}

			for (const item of extracted) {
				deviceMap[item.user] = deviceMap[item.user] || []
				deviceMap[item.user]?.push(item)
			}

			// Process each user's devices as a group for bulk LID migration
			for (const [user, userDevices] of Object.entries(deviceMap)) {
				const isLidUser = requestedLidUsers.has(user)

				// Process all devices for this user
				for (const item of userDevices) {
					const finalJid = isLidUser
						? jidEncode(user, item.server, item.device)
						: jidEncode(item.user, item.server, item.device)

					deviceResults.push({
						...item,
						jid: finalJid
					})

					logger.debug(
						{
							user: item.user,
							device: item.device,
							finalJid,
							usedLid: isLidUser
						},
						'Processed device with LID priority'
					)
				}
			}

			await devicesMutex.mutex(async () => {
				// The NodeCache implementations accept different entry formats for bulk writes.
				for (const [user, devices] of Object.entries(deviceMap)) {
					await userDevicesCache.set(user, devices)
				}
			})

			const userDeviceUpdates: { [userId: string]: string[] } = {}
			for (const [userId, devices] of Object.entries(deviceMap)) {
				if (devices && devices.length > 0) {
					userDeviceUpdates[userId] = devices.map(d => d.device?.toString() || '0')
				}
			}

			if (Object.keys(userDeviceUpdates).length > 0) {
				try {
					await authState.keys.set({ 'device-list': userDeviceUpdates })
					logger.debug(
						{ userCount: Object.keys(userDeviceUpdates).length },
						'stored user device lists for bulk migration'
					)
				} catch (error) {
					logger.warn({ error }, 'failed to store user device lists')
				}
			}
		}

		return deviceResults
	}

	/** Fetch all the devices we've to send a message to */
	const getUSyncDevices = async (jids: string[], useCache: boolean, ignoreZeroDevices: boolean) => {
		if (!useCache) logger.debug('not using cache for devices')
		const { deviceResults, toFetch } = await collectKnownDevices(jids, useCache)
		return [...deviceResults, ...(await fetchUSyncDevices(toFetch, ignoreZeroDevices))]
	}

	/**
	 * Update Member Label
	 */
	const updateMemberLabel = (jid: string, memberLabel: string) => {
		return relayMessage(
			jid,
			{
				protocolMessage: {
					type: proto.Message.ProtocolMessage.Type.GROUP_MEMBER_LABEL_CHANGE,
					memberLabel: {
						label: memberLabel?.slice(0, 30),
						labelTimestamp: unixTimestampSeconds()
					}
				}
			},
			{
				additionalNodes: [
					{
						tag: 'meta',
						attrs: {
							tag_reason: 'user_update',
							appdata: 'member_tag'
						},
						content: undefined
					}
				]
			}
		)
	}

	const assertSessions = async (jids: string[], force?: boolean) => {
		let didFetchNewSession = false
		const uniqueJids = [...new Set(jids)]
		const jidsRequiringFetch: string[] = []

		logger.debug({ jids }, 'assertSessions call with jids')

		for (const jid of uniqueJids) {
			if (!force) {
				const sessionValidation = await signalRepository.validateSession(jid)
				if (sessionValidation.exists) {
					continue
				}
			}

			jidsRequiringFetch.push(jid)
		}

		if (jidsRequiringFetch.length) {
			// LID if mapped, otherwise original
			const wireJids = [
				...jidsRequiringFetch.filter(jid => !!isLidUser(jid) || !!isHostedLidUser(jid)),
				...(
					(await signalRepository.lidMapping.getLIDsForPNs(
						jidsRequiringFetch.filter(jid => !!isPnUser(jid) || !!isHostedPnUser(jid))
					)) || []
				).map(a => a.lid)
			]

			logger.debug({ jidsRequiringFetch, wireJids }, 'fetching sessions')
			const result = await query({
				tag: 'iq',
				attrs: {
					xmlns: 'encrypt',
					type: 'get',
					to: S_WHATSAPP_NET
				},
				content: [
					{
						tag: 'key',
						attrs: {},
						content: wireJids.map(jid => {
							const attrs: { [key: string]: string } = { jid }
							if (force) attrs.reason = 'identity'
							return { tag: 'user', attrs }
						})
					}
				]
			})
			await parseAndInjectE2ESessions(result, signalRepository)
			didFetchNewSession = true
		}

		return didFetchNewSession
	}

	const sendPeerDataOperationMessage = async (
		pdoMessage: proto.Message.IPeerDataOperationRequestMessage
	): Promise<string> => {
		//TODO: for later, abstract the logic to send a Peer Message instead of just PDO - useful for App State Key Resync with phone
		if (!authState.creds.me?.id) {
			throw new Boom('Not authenticated')
		}

		const protocolMessage: proto.IMessage = {
			protocolMessage: {
				peerDataOperationRequestMessage: pdoMessage,
				type: proto.Message.ProtocolMessage.Type.PEER_DATA_OPERATION_REQUEST_MESSAGE
			}
		}

		const meJid = jidNormalizedUser(authState.creds.me.id)

		const msgId = await relayMessage(meJid, protocolMessage, {
			additionalAttributes: {
				category: 'peer',

				push_priority: 'high_force'
			},
			additionalNodes: [
				{
					tag: 'meta',
					attrs: { appdata: 'default' }
				}
			]
		})

		return msgId
	}

	const createParticipantNodes = async (
		recipientJids: string[],
		message: proto.IMessage,
		extraAttrs?: BinaryNode['attrs'],
		dsmMessage?: proto.IMessage
	) => {
		if (!recipientJids.length) {
			return { nodes: [] as BinaryNode[], shouldIncludeDeviceIdentity: false }
		}

		const patched = await patchMessageBeforeSending(message, recipientJids)
		const patchedMessages = Array.isArray(patched)
			? patched
			: recipientJids.map(jid => ({ recipientJid: jid, message: patched }))

		let shouldIncludeDeviceIdentity = false
		const meId = authState.creds.me!.id
		const meLid = authState.creds.me?.lid
		const meLidUser = meLid ? jidDecode(meLid)?.user : null

		const encryptionPromises = (patchedMessages as any).map(
			async ({ recipientJid: jid, message: patchedMessage }: any) => {
				try {
					if (!jid) return null

					let msgToEncrypt = patchedMessage

					if (dsmMessage) {
						const { user: targetUser } = jidDecode(jid)!
						const { user: ownPnUser } = jidDecode(meId)!
						const ownLidUser = meLidUser

						const isOwnUser = targetUser === ownPnUser || (ownLidUser && targetUser === ownLidUser)
						const isExactSenderDevice = jid === meId || (meLid && jid === meLid)

						if (isOwnUser && !isExactSenderDevice) {
							msgToEncrypt = dsmMessage
							logger.debug({ jid, targetUser }, 'Using DSM for own device')
						}
					}

					const bytes = encodeWAMessage(msgToEncrypt)
					const mutexKey = jid

					const node = await encryptionMutex.mutex(mutexKey, async () => {
						const { type, ciphertext } = await signalRepository.encryptMessage({ jid, data: bytes })

						if (type === 'pkmsg') {
							shouldIncludeDeviceIdentity = true
						}

						return {
							tag: 'to',
							attrs: { jid },
							content: [
								{
									tag: 'enc',
									attrs: { v: '2', type, ...(extraAttrs || {}) },
									content: ciphertext
								}
							]
						}
					})

					return node
				} catch (err) {
					logger.error({ jid, err }, 'Failed to encrypt for recipient')
					return null
				}
			}
		)

		const nodes = (await Promise.all(encryptionPromises)).filter(node => node !== null) as BinaryNode[]

		if (recipientJids.length > 0 && nodes.length === 0) {
			throw new Boom('All encryptions failed', { statusCode: 500 })
		}

		return { nodes, shouldIncludeDeviceIdentity }
	}

	type DirectDeviceLookup = {
		jids: Set<string>
		emptyResults: Set<string>
	}

	const relayMessageTransaction = async (
		jid: string,
		message: proto.IMessage,
		msgId: string,
		{
			participant,
			additionalAttributes,
			additionalNodes,
			useUserDevicesCache,
			useCachedGroupMetadata,
			statusJidList
		}: MessageRelayOptions,
		directDeviceLookup?: DirectDeviceLookup
	) => {
		const meId = assertMeId(authState.creds)
		const meLid = authState.creds.me?.lid
		const isRetryResend = Boolean(participant?.jid)
		let shouldIncludeDeviceIdentity = isRetryResend
		const statusJid = 'status@broadcast'

		const { user, server } = jidDecode(jid)!
		const isGroup = server === 'g.us'
		const isStatus = jid === statusJid
		const isLid = server === 'lid'
		const isNewsletter = server === 'newsletter'
		const isGroupOrStatus = isGroup || isStatus
		const finalJid = jid

		useUserDevicesCache = useUserDevicesCache !== false
		useCachedGroupMetadata = useCachedGroupMetadata !== false && !isStatus

		const participants: BinaryNode[] = []
		const destinationJid = !isStatus ? finalJid : statusJid
		const binaryNodeContent: BinaryNode[] = []
		const devices: DeviceWithJid[] = []
		let reportingMessage: proto.IMessage | undefined

		const meMsg: proto.IMessage = {
			deviceSentMessage: {
				destinationJid,
				message
			},
			messageContextInfo: message.messageContextInfo
		}

		const extraAttrs: BinaryNodeAttributes = {}

		if (participant) {
			if (!isGroup && !isStatus) {
				additionalAttributes = { ...additionalAttributes, device_fanout: 'false' }
			}

			const { user, device } = jidDecode(participant.jid)!
			devices.push({
				user,
				device,
				jid: participant.jid
			})
		}

		let issueTcToken: (() => void) | undefined

		const missingDevices = await authState.keys.transaction(async () => {
			if (directDeviceLookup) {
				const { deviceResults, toFetch } = await collectKnownDevices([...directDeviceLookup.jids], true)
				const pending = toFetch.filter(jid => !directDeviceLookup.emptyResults.has(jid))
				if (pending.length) return pending

				devices.push(...deviceResults)
			}

			const mediaType = getMediaType(message)
			if (mediaType) {
				extraAttrs['mediatype'] = mediaType
			}

			if (isNewsletter) {
				const patched = patchMessageBeforeSending ? await patchMessageBeforeSending(message, []) : message
				const bytes = encodeNewsletterMessage(patched as proto.IMessage)
				binaryNodeContent.push({
					tag: 'plaintext',
					attrs: {},
					content: bytes
				})
				const stanza: BinaryNode = {
					tag: 'message',
					attrs: {
						to: jid,
						id: msgId,
						type: getMessageType(message),
						...(additionalAttributes || {})
					},
					content: binaryNodeContent
				}
				logger.debug({ msgId }, `sending newsletter message to ${jid}`)
				await sendNode(stanza)
				return
			}

			if (normalizeMessageContent(message)?.pinInChatMessage || normalizeMessageContent(message)?.reactionMessage) {
				extraAttrs['decrypt-fail'] = 'hide' // todo: expand for reactions and other types
			}

			if (isGroupOrStatus && !isRetryResend) {
				const [groupData, senderKeyMap] = await Promise.all([
					(async () => {
						let groupData = useCachedGroupMetadata && cachedGroupMetadata ? await cachedGroupMetadata(jid) : undefined // todo: should we rely on the cache specially if the cache is outdated and the metadata has new fields?
						if (groupData && Array.isArray(groupData?.participants)) {
							logger.trace({ jid, participants: groupData.participants.length }, 'using cached group metadata')
						} else if (!isStatus) {
							groupData = await groupMetadata(jid) // TODO: start storing group participant list + addr mode in Signal & stop relying on this
						}

						return groupData
					})(),
					(async () => {
						if (!participant && !isStatus) {
							// what if sender memory is less accurate than the cached metadata
							// on participant change in group, we should do sender memory manipulation
							const result = await authState.keys.get('sender-key-memory', [jid]) // TODO: check out what if the sender key memory doesn't include the LID stuff now?
							return result[jid] || {}
						}

						return {}
					})()
				])

				const participantsList = groupData ? groupData.participants.map(p => p.id) : []

				if (groupData?.ephemeralDuration && groupData.ephemeralDuration > 0) {
					additionalAttributes = {
						...additionalAttributes,
						expiration: groupData.ephemeralDuration.toString()
					}
				}

				if (isStatus && statusJidList) {
					participantsList.push(...statusJidList)
				}

				const additionalDevices = await getUSyncDevices(participantsList, !!useUserDevicesCache, false)
				devices.push(...additionalDevices)

				if (isGroup) {
					additionalAttributes = {
						...additionalAttributes,
						addressing_mode: groupData?.addressingMode || 'lid'
					}
				}

				const patched = await patchMessageBeforeSending(message)
				if (Array.isArray(patched)) {
					throw new Boom('Per-jid patching is not supported in groups')
				}

				const bytes = encodeWAMessage(patched)
				reportingMessage = patched
				const groupAddressingMode = additionalAttributes?.['addressing_mode'] || groupData?.addressingMode || 'lid'
				const groupSenderIdentity = groupAddressingMode === 'lid' && meLid ? meLid : meId

				const { ciphertext, senderKeyDistributionMessage } = await signalRepository.encryptGroupMessage({
					group: destinationJid,
					data: bytes,
					meId: groupSenderIdentity
				})

				const senderKeyRecipients: string[] = []
				for (const device of devices) {
					const deviceJid = device.jid
					const hasKey = !!senderKeyMap[deviceJid]
					if (
						(!hasKey || !!participant) &&
						!isHostedLidUser(deviceJid) &&
						!isHostedPnUser(deviceJid) &&
						device.device !== 99
					) {
						//todo: revamp all this logic
						// the goal is to follow with what I said above for each group, and instead of a true false map of ids, we can set an array full of those the app has already sent pkmsgs
						senderKeyRecipients.push(deviceJid)
						senderKeyMap[deviceJid] = true
					}
				}

				if (senderKeyRecipients.length) {
					logger.debug({ senderKeyJids: senderKeyRecipients }, 'sending new sender key')

					const senderKeyMsg: proto.IMessage = {
						senderKeyDistributionMessage: {
							axolotlSenderKeyDistributionMessage: senderKeyDistributionMessage,
							groupId: destinationJid
						}
					}

					const senderKeySessionTargets = senderKeyRecipients
					await assertSessions(senderKeySessionTargets)

					const result = await createParticipantNodes(senderKeyRecipients, senderKeyMsg, extraAttrs)
					shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || result.shouldIncludeDeviceIdentity

					participants.push(...result.nodes)
				}

				binaryNodeContent.push({
					tag: 'enc',
					attrs: { v: '2', type: 'skmsg', ...extraAttrs },
					content: ciphertext
				})

				await authState.keys.set({ 'sender-key-memory': { [jid]: senderKeyMap } })
			} else {
				// ADDRESSING CONSISTENCY: Match own identity to conversation context
				// TODO: investigate if this is true
				let ownId = meId
				if (isLid && meLid) {
					ownId = meLid
					logger.debug({ to: jid, ownId }, 'Using LID identity for @lid conversation')
				} else {
					logger.debug({ to: jid, ownId }, 'Using PN identity for @s.whatsapp.net conversation')
				}

				const { user: ownUser } = jidDecode(ownId)!
				if (!participant) {
					const patchedForReporting = await patchMessageBeforeSending(message, [jid])
					reportingMessage = Array.isArray(patchedForReporting)
						? patchedForReporting.find(item => item.recipientJid === jid) || patchedForReporting[0]
						: patchedForReporting
				}

				if (directDeviceLookup) {
					logger.debug(
						{
							deviceCount: devices.length,
							devices: devices.map(d => `${d.user}:${d.device}@${jidDecode(d.jid)?.server}`)
						},
						'Device enumeration complete with unified addressing'
					)
				} else if (!isRetryResend) {
					const targetUserServer = isLid ? 'lid' : 's.whatsapp.net'
					devices.push({
						user,
						device: 0,
						jid: jidEncode(user, targetUserServer, 0)
					})

					if (user !== ownUser) {
						devices.push({
							user: ownUser,
							device: 0,
							jid: jidEncode(ownUser, targetUserServer, 0)
						})
					}
				}

				const allRecipients: string[] = []
				const meRecipients: string[] = []
				const otherRecipients: string[] = []
				const { user: mePnUser } = jidDecode(meId)!
				const { user: meLidUser } = meLid ? jidDecode(meLid)! : { user: null }

				for (const { user, jid } of devices) {
					const isExactSenderDevice = jid === meId || (meLid && jid === meLid)
					if (isExactSenderDevice) {
						logger.debug({ jid, meId, meLid }, 'Skipping exact sender device (whatsmeow pattern)')
						continue
					}

					// Check if this is our device (could match either PN or LID user)
					const isMe = user === mePnUser || user === meLidUser

					if (isMe) {
						meRecipients.push(jid)
					} else {
						otherRecipients.push(jid)
					}

					allRecipients.push(jid)
				}

				try {
					await assertSessions(allRecipients)
				} catch (error) {
					if (directDeviceLookup && isBoom(error) && error.data === 406 && error.message === 'not-acceptable') {
						throw new DeviceSessionQueryRejected(error)
					}

					throw error
				}

				const [
					{ nodes: meNodes, shouldIncludeDeviceIdentity: s1 },
					{ nodes: otherNodes, shouldIncludeDeviceIdentity: s2 }
				] = await Promise.all([
					// For own devices: use DSM if available (1:1 chats only)
					createParticipantNodes(meRecipients, meMsg || message, extraAttrs),
					createParticipantNodes(otherRecipients, message, extraAttrs, meMsg)
				])
				participants.push(...meNodes)
				participants.push(...otherNodes)

				if (meRecipients.length > 0 || otherRecipients.length > 0) {
					extraAttrs['phash'] = generateParticipantHashV2([...meRecipients, ...otherRecipients])
				}

				shouldIncludeDeviceIdentity = shouldIncludeDeviceIdentity || s1 || s2
			}

			if (isRetryResend) {
				const isParticipantLid = isLidUser(participant!.jid)
				const isMe = areJidsSameUser(participant!.jid, isParticipantLid ? meLid : meId)

				let messageToSend = message
				if (isGroupOrStatus) {
					let groupSenderIdentity: string | undefined
					if (meLid && (await signalRepository.hasSenderKey({ group: destinationJid, meId: meLid }))) {
						groupSenderIdentity = meLid
					} else if (await signalRepository.hasSenderKey({ group: destinationJid, meId })) {
						groupSenderIdentity = meId
					}

					if (groupSenderIdentity) {
						try {
							const skdm = await signalRepository.getSenderKeyDistributionMessage({
								group: destinationJid,
								meId: groupSenderIdentity
							})
							messageToSend = {
								...message,
								senderKeyDistributionMessage: {
									groupId: destinationJid,
									axolotlSenderKeyDistributionMessage: skdm
								}
							}
						} catch (err) {
							logger.warn({ err, jid: destinationJid }, 'failed to build SKDM for retry, sending without it')
						}
					}
				}

				const encodedMessageToSend = isMe
					? encodeWAMessage({
							deviceSentMessage: {
								destinationJid,
								message: messageToSend
							}
						})
					: encodeWAMessage(messageToSend)

				const { type, ciphertext: encryptedContent } = await signalRepository.encryptMessage({
					data: encodedMessageToSend,
					jid: participant!.jid
				})

				binaryNodeContent.push({
					tag: 'enc',
					attrs: {
						v: '2',
						type,
						count: participant!.count.toString()
					},
					content: encryptedContent
				})
			}

			if (participants.length) {
				if (additionalAttributes?.['category'] === 'peer') {
					const peerNode = participants[0]?.content?.[0] as BinaryNode
					if (peerNode) {
						binaryNodeContent.push(peerNode) // push only enc
					}
				} else {
					binaryNodeContent.push({
						tag: 'participants',
						attrs: {},

						content: participants
					})
				}
			}

			const stanza: BinaryNode = {
				tag: 'message',
				attrs: {
					id: msgId,
					to: destinationJid,
					type: getMessageType(message),
					...(additionalAttributes || {})
				},
				content: binaryNodeContent
			}

			// if the participant to send to is explicitly specified (generally retry recp)
			// ensure the message is only sent to that person
			// if a retry receipt is sent to everyone -- it'll fail decryption for everyone else who received the msg
			if (participant) {
				if (isJidGroup(destinationJid)) {
					stanza.attrs.to = destinationJid
					stanza.attrs.participant = participant.jid
				} else if (areJidsSameUser(participant.jid, meId)) {
					stanza.attrs.to = participant.jid
					stanza.attrs.recipient = destinationJid
				} else {
					stanza.attrs.to = participant.jid
				}
			} else {
				stanza.attrs.to = destinationJid
			}

			if (shouldIncludeDeviceIdentity) {
				;(stanza.content as BinaryNode[]).push({
					tag: 'device-identity',
					attrs: {},
					content: encodeSignedDeviceIdentity(authState.creds.account!, true)
				})

				logger.debug({ jid }, 'adding device identity')
			}

			if (
				!isNewsletter &&
				!isRetryResend &&
				reportingMessage?.messageContextInfo?.messageSecret &&
				shouldIncludeReportingToken(reportingMessage)
			) {
				try {
					const encoded = encodeWAMessage(reportingMessage)
					const reportingKey: WAMessageKey = {
						id: msgId,
						fromMe: true,
						remoteJid: destinationJid,
						participant: participant?.jid
					}
					const reportingNode = await getMessageReportingToken(encoded, reportingMessage, reportingKey)
					if (reportingNode) {
						;(stanza.content as BinaryNode[]).push(reportingNode)
						logger.trace({ jid }, 'added reporting token to message')
					}
				} catch (error: any) {
					logger.warn({ jid, trace: error?.stack }, 'failed to attach reporting token')
				}
			}

			// WA Web never attaches tctoken to peer (AppStateSync) messages — server rejects with 479
			const isPeerMessage = additionalAttributes?.['category'] === 'peer'
			const is1on1Send = !isGroup && !isRetryResend && !isStatus && !isNewsletter && !isPeerMessage

			// Resolve destination to LID for tctoken storage — matches Signal session key pattern
			const tcTokenJid = is1on1Send ? await resolveTcTokenJid(destinationJid, getLIDForPN) : destinationJid
			const contactTcTokenData = is1on1Send ? await authState.keys.get('tctoken', [tcTokenJid]) : {}
			const existingTokenEntry = contactTcTokenData[tcTokenJid]
			let tcTokenBuffer = existingTokenEntry?.token

			// Treat expired tokens the same as missing — clear from cache
			if (tcTokenBuffer?.length && isTcTokenExpired(existingTokenEntry?.timestamp)) {
				logger.debug({ jid: destinationJid, timestamp: existingTokenEntry?.timestamp }, 'tctoken expired, clearing')
				tcTokenBuffer = undefined
				// Preserve senderTimestamp so the fire-and-forget issuance dedupe survives cleanup.
				const cleared =
					existingTokenEntry?.senderTimestamp !== undefined
						? { token: Buffer.alloc(0), senderTimestamp: existingTokenEntry.senderTimestamp }
						: null
				try {
					await authState.keys.set({ tctoken: { [tcTokenJid]: cleared } })
				} catch (err: any) {
					logger.debug({ jid: destinationJid, err: err?.message }, 'failed to persist tctoken expiry cleanup')
				}
			}

			if (tcTokenBuffer?.length && sock.serverProps.privacyTokenOn1to1) {
				;(stanza.content as BinaryNode[]).push({
					tag: 'tctoken',
					attrs: {},
					content: tcTokenBuffer
				})
			}

			if (additionalNodes && additionalNodes.length > 0) {
				;(stanza.content as BinaryNode[]).push(...additionalNodes)
			}

			logger.debug({ msgId }, `sending message to ${participants.length} devices`)

			await sendNode(stanza)

			// Fire-and-forget: issue our token to the contact AFTER message send.
			// WA Web skips protocol messages and PSA/bot contacts (TcTokenChatAction: isRegularUser)
			const isProtocolMsg = !!normalizeMessageContent(message)?.protocolMessage
			const isBotOrPSA = destinationJid === PSA_WID || isJidBot(destinationJid) || isJidMetaAI(destinationJid)
			if (
				is1on1Send &&
				!isProtocolMsg &&
				!isBotOrPSA &&
				shouldSendNewTcToken(existingTokenEntry?.senderTimestamp) &&
				!inFlightTcTokenIssuance.has(tcTokenJid)
			) {
				const issueTimestamp = unixTimestampSeconds()
				const getPNForLID = signalRepository.lidMapping.getPNForLID.bind(signalRepository.lidMapping)
				issueTcToken = () => {
					if (inFlightTcTokenIssuance.has(tcTokenJid)) return
					inFlightTcTokenIssuance.add(tcTokenJid)
					resolveIssuanceJid(destinationJid, sock.serverProps.lidTrustedTokenIssueToLid, getLIDForPN, getPNForLID)
						.then(issueJid => issuePrivacyTokens([issueJid], issueTimestamp))
						.then(async result => {
							await authState.keys.transaction(async () => {
								await storeTcTokensFromIqResult({
									result,
									fallbackJid: tcTokenJid,
									keys: authState.keys,
									getLIDForPN
								})

								const currentData = await authState.keys.get('tctoken', [tcTokenJid])
								const currentEntry = currentData[tcTokenJid]
								const indexWrite = await buildMergedTcTokenIndexWrite(authState.keys, [tcTokenJid])
								await authState.keys.set({
									tctoken: {
										[tcTokenJid]: {
											token: Buffer.alloc(0),
											...currentEntry,
											senderTimestamp: issueTimestamp
										},
										...indexWrite
									}
								})
							}, 'tctoken-issuance:' + tcTokenJid)
						})
						.catch(err => {
							logger.debug({ jid: destinationJid, err: err?.message }, 'fire-and-forget tctoken issuance failed')
						})
						.finally(() => {
							inFlightTcTokenIssuance.delete(tcTokenJid)
						})
				}
			}

			// Add message to retry cache if enabled
			if (messageRetryManager && !participant) {
				messageRetryManager.addRecentMessage(destinationJid, msgId, message)
			}
		}, meId)

		if (issueTcToken) authState.keys.runOutsideTransaction(issueTcToken)

		return missingDevices
	}

	const relayMessage = async (jid: string, message: proto.IMessage, options: MessageRelayOptions) => {
		const meId = assertMeId(authState.creds)
		const msgId = options.messageId || generateMessageIDV2(meId)
		if (
			isJidGroup(jid) ||
			isJidStatusBroadcast(jid) ||
			isJidNewsletter(jid) ||
			options.participant?.jid ||
			options.additionalAttributes?.['category'] === 'peer'
		) {
			await relayMessageTransaction(jid, message, msgId, options)
			return msgId
		}

		const nested = authState.keys.isInTransaction()
		const resolveDevicesAndRelay = async () => {
			const meLid = authState.creds.me?.lid
			const senderUsers = new Set([jidDecode(meId)?.user, meLid ? jidDecode(meLid)?.user : undefined])
			const senderIdentity =
				isLidUser(jid) && meLid
					? jidEncode(jidDecode(meLid)!.user, 'lid')
					: jidEncode(jidDecode(meId)!.user, 's.whatsapp.net')
			const lookup: DirectDeviceLookup = { jids: new Set([senderIdentity, jid]), emptyResults: new Set() }
			let refreshedRejectedDevices = false
			let refreshes = 0
			for (;;) {
				let missing: string[] | undefined
				try {
					missing = await relayMessageTransaction(jid, message, msgId, options, lookup)
				} catch (error) {
					if (!(error instanceof DeviceSessionQueryRejected)) throw error
					if (nested || refreshedRejectedDevices || jidDecode(jid)?.device !== undefined) throw error.rejection

					// The pre-key query failed before dispatch. Refresh only this recipient outside the key transaction.
					refreshedRejectedDevices = true
					logger.debug({ msgId }, 'refreshing devices after a rejected pre-key query')
					const refreshed = await fetchUSyncDevices([jidNormalizedUser(jid)], false).catch(() => {
						logger.warn({ msgId }, 'could not refresh devices after a rejected pre-key query')
						return []
					})
					if (!refreshed.length) throw error.rejection

					const refreshedJids = refreshed.map(device => jidNormalizedUser(device.jid))
					const refreshedUsers = new Set(refreshed.map(device => device.user))
					const senderJids: string[] = []
					await devicesMutex.mutex(async () => {
						for (const target of lookup.jids) {
							const user = jidDecode(target)?.user
							if (!user) continue
							if (senderUsers.has(user)) {
								senderJids.push(target)
							} else if (!refreshedUsers.has(user)) {
								await userDevicesCache.del(user)
							}
						}
					})
					lookup.jids = new Set([...senderJids, ...refreshedJids])
					continue
				}

				if (!missing) return msgId
				if (refreshes++ === 2) {
					throw new Boom('Device list changed during send', { statusCode: 503 })
				}

				// Defer USync until the relay transaction has returned so other recipients can proceed.
				logger.debug({ msgId }, 'resolving devices for direct relay')
				const fetched = await fetchUSyncDevices(missing, false)
				const fetchedUsers = new Set(fetched.map(device => device.user))
				for (const target of missing) {
					if (!fetchedUsers.has(jidDecode(target)!.user)) lookup.emptyResults.add(target)
				}

				// USync can return devices under the other address of the same user.
				const requestedUsers = new Set([...lookup.jids].map(target => jidDecode(target)?.user))
				for (const device of fetched) {
					if (!requestedUsers.has(device.user)) lookup.jids.add(jidNormalizedUser(device.jid))
				}
			}
		}

		// A nested relay already holds a key transaction and must not queue behind a send waiting to acquire it.
		if (nested) return resolveDevicesAndRelay()

		// Resolve local aliases in arrival order. Release this lock as soon as the send joins its recipient's queue.
		const { pending } = await directRelayIdentityMutex.mutex(async () => {
			const normalized = jidNormalizedUser(jid)
			const user = jidDecode(normalized)?.user
			const phone =
				(isLidUser(normalized) || isHostedLidUser(normalized)) && user
					? await signalRepository.lidMapping.getPNForLID(jidEncode(user, 'lid'))
					: null
			const recipientKey =
				isHostedPnUser(normalized) && user ? jidEncode(user, 's.whatsapp.net') : jidNormalizedUser(phone ?? normalized)
			return { pending: directRelayMutex.mutex(recipientKey, resolveDevicesAndRelay) }
		})

		return pending
	}

	const getMessageType = (message: proto.IMessage) => {
		const normalizedMessage = normalizeMessageContent(message)
		if (!normalizedMessage) return 'text'

		if (normalizedMessage.reactionMessage || normalizedMessage.encReactionMessage) {
			return 'reaction'
		}

		if (
			normalizedMessage.pollCreationMessage ||
			normalizedMessage.pollCreationMessageV2 ||
			normalizedMessage.pollCreationMessageV3 ||
			normalizedMessage.pollUpdateMessage
		) {
			return 'poll'
		}

		if (normalizedMessage.eventMessage) {
			return 'event'
		}

		if (getMediaType(normalizedMessage) !== '') {
			return 'media'
		}

		return 'text'
	}

	const getMediaType = (message: proto.IMessage) => {
		if (message.imageMessage) {
			return 'image'
		} else if (message.videoMessage) {
			return message.videoMessage.gifPlayback ? 'gif' : 'video'
		} else if (message.audioMessage) {
			return message.audioMessage.ptt ? 'ptt' : 'audio'
		} else if (message.contactMessage) {
			return 'vcard'
		} else if (message.documentMessage) {
			return 'document'
		} else if (message.contactsArrayMessage) {
			return 'contact_array'
		} else if (message.liveLocationMessage) {
			return 'livelocation'
		} else if (message.stickerMessage) {
			return 'sticker'
		} else if (message.listMessage) {
			return 'list'
		} else if (message.listResponseMessage) {
			return 'list_response'
		} else if (message.buttonsResponseMessage) {
			return 'buttons_response'
		} else if (message.orderMessage) {
			return 'order'
		} else if (message.productMessage) {
			return 'product'
		} else if (message.interactiveResponseMessage) {
			return 'native_flow_response'
		} else if (message.groupInviteMessage) {
			return 'url'
		}

		return ''
	}

	const issuePrivacyTokens = async (jids: string[], timestamp?: number) => {
		const t = (timestamp ?? unixTimestampSeconds()).toString()
		const result = await query({
			tag: 'iq',
			attrs: {
				to: S_WHATSAPP_NET,
				type: 'set',
				xmlns: 'privacy'
			},
			content: [
				{
					tag: 'tokens',
					attrs: {},
					content: jids.map(jid => ({
						tag: 'token',
						attrs: {
							jid: jidNormalizedUser(jid),
							t,
							type: 'trusted_contact'
						}
					}))
				}
			]
		})

		return result
	}

	const waUploadToServer = getWAUploadToServer(config, refreshMediaConn)

	const waitForMsgMediaUpdate = bindWaitForEvent(ev, 'messages.media-update')

	registerSocketEndHandler(() => {
		if (!config.userDevicesCache && userDevicesCache.close) {
			userDevicesCache.close()
		}

		if (messageRetryManager) {
			messageRetryManager.clear()
		}
	})

	const downloadMediaMessageWithHost = <Type extends 'buffer' | 'stream'>(
		...[message, type, options, ctx]: Parameters<typeof downloadMediaMessage<Type>>
	) => downloadMediaMessage<Type>(message, type, { ...options, host: options?.host ?? getMediaHost() }, ctx)

	const downloadContentFromMessageWithHost: typeof downloadContentFromMessage = (message, type, opts = {}) =>
		downloadContentFromMessage(message, type, { ...opts, host: opts.host ?? getMediaHost() })

	return {
		...sock,
		userDevicesCache,
		devicesMutex,
		issuePrivacyTokens,
		assertSessions,
		relayMessage,
		sendReceipt,
		sendReceipts,
		readMessages,
		refreshMediaConn,
		getMediaHost,
		getMediaUrl: (directPath: string) => getUrlFromDirectPath(directPath, getMediaHost()),
		downloadMediaMessage: downloadMediaMessageWithHost,
		downloadContentFromMessage: downloadContentFromMessageWithHost,
		waUploadToServer,
		fetchPrivacySettings,
		sendPeerDataOperationMessage,
		createParticipantNodes,
		getUSyncDevices,
		messageRetryManager,
		updateMemberLabel,
		updateMediaMessage: async (message: WAMessage) => {
			const content = assertMediaContent(message.message)
			const mediaKey = content.mediaKey!
			const meId = authState.creds.me!.id
			const node = encryptMediaRetryRequest(message.key, mediaKey, meId)

			let error: Error | undefined = undefined
			await Promise.all([
				sendNode(node),
				waitForMsgMediaUpdate(async update => {
					const result = update.find(c => c.key.id === message.key.id)
					if (result) {
						if (result.error) {
							error = result.error
						} else {
							try {
								const media = decryptMediaRetryData(result.media!, mediaKey, result.key.id!)
								if (media.result !== proto.MediaRetryNotification.ResultType.SUCCESS) {
									const resultStr = proto.MediaRetryNotification.ResultType[media.result!]
									throw new Boom(`Media re-upload failed by device (${resultStr})`, {
										data: media,
										statusCode: getStatusCodeForMediaRetry(media.result!) || 404
									})
								}

								content.directPath = media.directPath
								content.url = getUrlFromDirectPath(content.directPath!, getMediaHost())

								logger.debug({ directPath: media.directPath, key: result.key }, 'media update successful')
							} catch (err: any) {
								error = err
							}
						}

						return true
					}
				})
			])

			if (error) {
				throw error
			}

			ev.emit('messages.update', [{ key: message.key, update: { message: message.message } }])

			return message
		},
		sendMessage: async (jid: string, content: AnyMessageContent, options: MiscMessageGenerationOptions = {}) => {
			const userJid = authState.creds.me!.id
			if (
				typeof content === 'object' &&
				'disappearingMessagesInChat' in content &&
				typeof content['disappearingMessagesInChat'] !== 'undefined' &&
				isJidGroup(jid)
			) {
				const { disappearingMessagesInChat } = content
				const value =
					typeof disappearingMessagesInChat === 'boolean'
						? disappearingMessagesInChat
							? WA_DEFAULT_EPHEMERAL
							: 0
						: disappearingMessagesInChat
				await groupToggleEphemeral(jid, value)
			} else {
				const fullMsg = await generateWAMessage(jid, content, {
					logger,
					userJid,
					getUrlInfo: text =>
						getUrlInfo(text, {
							thumbnailWidth: linkPreviewImageThumbnailWidth,
							fetchOpts: {
								timeout: 3_000,
								...(httpRequestOptions || {})
							},
							logger,
							uploadImage: generateHighQualityLinkPreview ? waUploadToServer : undefined
						}),
					//TODO: CACHE
					getProfilePicUrl: sock.profilePictureUrl,
					getCallLink: sock.createCallLink,
					upload: waUploadToServer,
					mediaCache: config.mediaCache,
					options: config.options,
					messageId: generateMessageIDV2(sock.user?.id),
					...options
				})
				const isEventMsg = 'event' in content && !!content.event
				const isDeleteMsg = 'delete' in content && !!content.delete
				const isEditMsg = 'edit' in content && !!content.edit
				const isPinMsg = 'pin' in content && !!content.pin
				const isPollMessage = 'poll' in content && !!content.poll
				const additionalAttributes: BinaryNodeAttributes = {}
				const additionalNodes: BinaryNode[] = []
				// required for delete
				if (isDeleteMsg) {
					// if the chat is a group, and I am not the author, then delete the message as an admin
					if (isJidGroup(content.delete?.remoteJid as string) && !content.delete?.fromMe) {
						additionalAttributes.edit = '8'
					} else {
						additionalAttributes.edit = '7'
					}
				} else if (isEditMsg) {
					additionalAttributes.edit = '1'
				} else if (isPinMsg) {
					additionalAttributes.edit = '2'
				} else if (isPollMessage) {
					additionalNodes.push({
						tag: 'meta',
						attrs: {
							polltype: 'creation'
						}
					} as BinaryNode)
				} else if (isEventMsg) {
					additionalNodes.push({
						tag: 'meta',
						attrs: {
							event_type: 'creation'
						}
					} as BinaryNode)
				}

				await relayMessage(jid, fullMsg.message!, {
					messageId: fullMsg.key.id!,
					useCachedGroupMetadata: options.useCachedGroupMetadata,
					additionalAttributes,
					statusJidList: options.statusJidList,
					additionalNodes
				})
				if (config.emitOwnEvents) {
					process.nextTick(async () => {
						await messageMutex.mutex(() => upsertMessage(fullMsg, 'append'))
					})
				}

				return fullMsg
			}
		}
	}
}
