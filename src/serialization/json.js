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
  return Buffer.from(JSON.stringify(data))
}

function unserialize (data) {
  return JSON.parse(data.toString())
}
