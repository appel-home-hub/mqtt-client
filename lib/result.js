const ok = value => ({ ok: true, value })
const err = error => ({ ok: false, error })

const tryCatch = fn => (...args) => {
	try { return ok(fn(...args)) }
	catch (error) { return err(error) }
}

const tryCatchAsync = fn => async (...args) => {
	try { return ok(await fn(...args)) }
	catch (error) { return err(error) }
}

module.exports = { ok, err, tryCatch, tryCatchAsync }
