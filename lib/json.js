const { tryCatch } = require('./result')

const parseJson = tryCatch(text => JSON.parse(text))

module.exports = { parseJson }
