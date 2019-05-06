module.exports = {
  serialization: {
    serialize,
    unserialize,
  },
}

function serialize (data) {
  return Buffer.from(JSON.stringify(data))
}

function unserialize (data) {
  return JSON.parse(data.toString())
}
