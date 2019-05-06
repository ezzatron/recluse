module.exports = {
  serialization: {
    copy,
    serialize,
    unserialize,
  },
}

function copy (data) {
  return JSON.parse(JSON.stringify(data))
}

function serialize (data) {
  if (typeof data === 'undefined') return null

  return Buffer.from(JSON.stringify(data))
}

function unserialize (data) {
  if (data === null) return undefined
  if (typeof data.toString !== 'function') throw new Error('Invalid serialized data')

  return JSON.parse(data.toString())
}
