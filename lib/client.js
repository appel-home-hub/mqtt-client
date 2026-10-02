const mqtt = require('mqtt')

const { ok, err, tryCatch, tryCatchAsync } = require('./result')
const { matchTopic } = require('./topic')

const REFUSED = 128

const defaults = {
	cacheFilter: 'stat/h/#',
	settleMs: 500,
	readyTimeoutMs: 10_000
}

const configFromEnv = env => ({
	host: env.MQTT_HOST || 'mqtt://localhost:1883',
	username: env.MQTT_USERNAME,
	password: env.MQTT_PASSWORD,
	clientId: env.MQTT_CLIENT_ID
})

const delay = ms => new Promise(resolve => setTimeout(resolve, ms))

// like delay, but doesn't keep the process alive on its own
const backgroundDelay = ms => new Promise(resolve => setTimeout(resolve, ms).unref())

const connect = ({ host, username, password, clientId }, suffix = '') =>
	mqtt.connect(host, { username, password, clientId: clientId && `${clientId}${suffix}` })

const createClient = (options = {}) => {
	const config = { ...defaults, ...configFromEnv(process.env), ...options }
	const { cacheFilter, settleMs, readyTimeoutMs } = config

	// handlers share one connection; the state cache gets its own so its catch-all
	// filter never overlaps a handler's filter (brokers may deliver overlaps twice)
	const connection = connect(config)
	const cacheConnection = cacheFilter ? connect(config, '-cache') : undefined

	const states = new Map()
	let handlers = []
	let listeners = []
	let lastRetainedAt = Date.now()

	// --- events ---

	const emit = (event, payload) => listeners
		.filter(entry => entry.event === event)
		.forEach(({ listener }) => listener(payload))

	const on = (event, listener) => {
		const entry = { event, listener }
		listeners = [...listeners, entry]
		return () => { listeners = listeners.filter(other => other !== entry) }
	}

	// a throwing or rejecting handler is reported as an 'error' event instead of
	// breaking delivery to the other handlers
	const runHandler = (handler, event) => {
		const result = tryCatch(handler)(event)
		if (!result.ok) return emit('error', result.error)
		Promise.resolve(result.value).catch(error => emit('error', error))
	}

	// --- message routing ---

	const route = (topic, payload, { retain }) => {
		const message = payload.toString()
		const retained = Boolean(retain)

		handlers.forEach(({ filter, handler }) => {
			const wildcards = matchTopic(filter, topic)
			if (wildcards) runHandler(handler, { topic, message, wildcards, retained })
		})
	}

	// an empty retained message clears the topic; an empty live message is a
	// "please re-publish" request, not a state, so it leaves the cache alone
	const cache = (topic, payload, { retain }) => {
		const message = payload.toString()
		if (retain) lastRetainedAt = Date.now()
		if (message === '' && !retain) return
		if (message === '') return states.delete(topic)
		states.set(topic, message)
	}

	connection.on('message', route)
	connection.on('connect', () => emit('connect'))
	connection.on('close', () => emit('disconnect'))
	connection.on('error', error => emit('error', error))

	cacheConnection?.on('message', cache)
	cacheConnection?.on('error', error => emit('error', error))

	// --- broker adapters ---

	const subscribeAdapter = tryCatchAsync(filter => connection.subscribeAsync(filter))
	const unsubscribeAdapter = tryCatchAsync(filter => connection.unsubscribeAsync(filter))
	const cacheSubscribeAdapter = tryCatchAsync(filter => cacheConnection.subscribeAsync(filter))

	const checkGranted = (filter, result) => {
		if (!result.ok) return result
		const refused = result.value.some(({ qos }) => qos === REFUSED)
		if (!refused) return ok()
		const message = `Broker refused subscription to '${filter}'`
		return err(new Error(message))
	}

	// --- subscriptions ---

	const unsubscribe = async entry => {
		handlers = handlers.filter(other => other !== entry)
		const stillUsed = handlers.some(({ filter }) => filter === entry.filter)
		if (stillUsed) return ok()
		return unsubscribeAdapter(entry.filter)
	}

	const subscribe = async (filter, handler) => {
		const entry = { filter, handler }
		const isNewFilter = !handlers.some(other => other.filter === filter)
		handlers = [...handlers, entry]

		if (!isNewFilter) return ok(() => unsubscribe(entry))

		const subscribed = checkGranted(filter, await subscribeAdapter(filter))
		if (subscribed.ok) return ok(() => unsubscribe(entry))

		handlers = handlers.filter(other => other !== entry)
		return subscribed
	}

	// --- publishing ---

	const publish = tryCatchAsync((topic, message, { retain = false, qos = 0 } = {}) =>
		connection.publishAsync(topic, `${message}`, { retain, qos }))

	// --- state cache ---

	const getState = topic => states.get(topic)

	const getStates = (filter = '#') => [...states]
		.map(([topic, message]) => ({ topic, message, wildcards: matchTopic(filter, topic) }))
		.filter(({ wildcards }) => wildcards)

	// --- readiness ---

	const connected = () => connection.connected
		? Promise.resolve(ok())
		: new Promise(resolve => connection.once('connect', () => resolve(ok())))

	// retained messages arrive right after the subscription is granted; the cache
	// counts as warm once none have arrived for `settleMs`
	const waitForQuiet = async () => {
		const quietFor = Date.now() - lastRetainedAt
		if (quietFor >= settleMs) return ok()
		await delay(settleMs - quietFor)
		return waitForQuiet()
	}

	const warmCache = async () => {
		if (!cacheConnection) return connected()
		const subscribed = checkGranted(cacheFilter, await cacheSubscribeAdapter(cacheFilter))
		if (!subscribed.ok) return subscribed
		lastRetainedAt = Date.now()
		return waitForQuiet()
	}

	const timeout = async ms => {
		await backgroundDelay(ms)
		const message = `MQTT client not ready after ${ms}ms`
		return err(new Error(message))
	}

	const readiness = Promise.race([warmCache(), timeout(readyTimeoutMs)])
	const ready = () => readiness

	// --- shutdown ---

	const end = tryCatchAsync(() => Promise.all([
		connection.endAsync(),
		cacheConnection?.endAsync()
	]))

	return { publish, subscribe, getState, getStates, ready, on, end }
}

module.exports = { createClient }
