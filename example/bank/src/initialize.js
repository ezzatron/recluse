const {initializeSchema} = require('./schema.js')

module.exports = async function initialize (pgClient) {
  await initializeSchema(pgClient)
}
