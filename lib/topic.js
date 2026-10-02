// Returns the wildcard captures when `topic` matches `filter`, otherwise undefined.
// `+` captures one level; `#` captures the remaining levels joined with '/'.
const matchTopic = (filter, topic) => {
	const filterLevels = filter.split('/')
	const topicLevels = topic.split('/')

	const step = (index, captures) => {
		const level = filterLevels[index]
		if (level === '#') return [...captures, topicLevels.slice(index).join('/')]
		if (index === filterLevels.length) return index === topicLevels.length ? captures : undefined
		if (index === topicLevels.length) return undefined
		if (level === '+') return step(index + 1, [...captures, topicLevels[index]])
		return level === topicLevels[index] ? step(index + 1, captures) : undefined
	}

	return step(0, [])
}

module.exports = { matchTopic }
