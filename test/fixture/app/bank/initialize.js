const {initializeSchema} = require('./schema.js')

module.exports = function createInitialize (schemaName) {
  return async function initialize (pgClient) {
    await initializeSchema(pgClient, schemaName)
  }
}
