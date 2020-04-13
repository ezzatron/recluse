const {initializeSchema} = require('./schema.js')

module.exports = function createInitialize (schemaName) {
  return async function initialize (client) {
    await initializeSchema(client, schemaName)
  }
}
