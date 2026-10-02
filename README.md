# mqtt-client

Shared MQTT plumbing for Home Hub components: one connection for publishing and subscriptions, topic routing with wildcards, and a cache of retained `stat/h/#` state.

This replaces the MQTT half of `state-manager`. Automation enable/settings (`isEnabled`, `getSettings`) belong in a separate `automation-config` library.

## Install (as a submodule)

```bash
git submodule add https://github.com/appel-home-hub/mqtt-client.git submodules/mqtt-client
```

```json
"dependencies": {
	"mqtt-client": "file:./submodules/mqtt-client"
}
```

## Usage

```javascript
const { createClient, parseJson } = require('mqtt-client')

const client = createClient()

client.on('error', error => console.error('[mqtt]', error.message))

const main = async () => {
	const ready = await client.ready()
	if (!ready.ok) return console.error(ready.error.message)

	await client.subscribe('cmnd/h/garage/door/+', ({ message, wildcards }) => {
		const [ door ] = wildcards
		const state = client.getState(`stat/h/garage/door/${door}`) || 'closed'
		console.log(`door ${door} is ${state}, received ${message}`)
	})

	await client.publish('stat/h/example/status', 'online', { retain: true })
}

main()
```

## Configuration

`createClient(options)` reads these from the environment; anything in `options` wins.

| Env var | Option | Default |
| --- | --- | --- |
| `MQTT_HOST` | `host` | `mqtt://localhost:1883` |
| `MQTT_USERNAME` | `username` | none |
| `MQTT_PASSWORD` | `password` | none |
| `MQTT_CLIENT_ID` | `clientId` | random |
| | `cacheFilter` | `stat/h/#` (`false` turns the cache off) |
| | `settleMs` | `500` |
| | `readyTimeoutMs` | `10000` |

## API

Everything that can fail returns a Result: `{ ok: true, value }` or `{ ok: false, error }`. Nothing throws.

| Function | Returns | Notes |
| --- | --- | --- |
| `publish(topic, message, { retain, qos })` | `Promise<Result>` | `message` is converted to a string. |
| `subscribe(filter, handler)` | `Promise<Result<unsubscribe>>` | `handler({ topic, message, wildcards, retained })`. `unsubscribe()` also returns `Promise<Result>`. |
| `getState(topic)` | `string \| undefined` | Exact topic only. |
| `getStates(filter = '#')` | `{ topic, message, wildcards }[]` | Always an array, with or without wildcards. |
| `ready()` | `Promise<Result>` | Resolves once retained state has loaded into the cache (or once connected, when the cache is off). |
| `on(event, listener)` | `off()` | Events: `connect`, `disconnect`, `error`. |
| `end()` | `Promise<Result>` | Closes all connections. |
| `parseJson(text)` | `Result` | Standalone export. |

## Behaviour notes

- **Wildcards:** `+` captures one level and `#` captures the rest joined with `/`. For `stat/h/+/light/#` and `stat/h/kitchen/light/main/POWER`, `wildcards` is `['kitchen', 'main/POWER']`.
- **Connections:** a client opens two connections, one for publishing and handlers, and one that only feeds the cache. This stops the cache's catch-all filter from overlapping handler filters. Some brokers (Mosquitto 2) deliver a message once per matching subscription. That means two overlapping filters *within one client* (for example `stat/h/#` and `stat/h/+/rf`) can still run each handler twice, so avoid them.
- **Cache:** the cache only holds topics under `cacheFilter`. It fills asynchronously, so `await client.ready()` before reading it at startup. An empty **retained** message removes a topic. An empty **live** message (a "please re-publish" request) leaves the cached value alone.
- **Handler errors:** if a handler throws or rejects, the error goes to `error` listeners and the other handlers still run.
- **Disconnects:** `disconnect` can fire again on each failed reconnect attempt. Subscriptions are restored automatically after reconnecting.
