const {basename} = require('path')
const {Client} = require('pg')

const {executeCommands: executeCommandsRaw} = require('../../../src/command.js')
const {serialization} = require('../../../src/serialization/json.js')

module.exports = {
  accountIdByName,
  die,
  executeCommands,
  runAsync,
  withClient,
}

async function accountIdByName (pgClient, name) {
  const result = await pgClient.query('SELECT id FROM bank.account WHERE name = $1', [name])

  return result.rowCount > 0 ? result.rows[0].id : null
}

function die (message) {
  console.log(message) // eslint-disable-line no-console

  process.exit(1)
}

async function executeCommands (pgClient, ...commands) {
  const binaryName = basename(process.argv[1])
  await executeCommandsRaw(serialization, pgClient, `cli.${binaryName}`, commands)
}

function runAsync (fn) {
  fn.catch(error => {
    console.error(error.stack) // eslint-disable-line no-console

    process.exit(1)
  })
}

async function withClient (fn) {
  const pgClient = new Client()
  await pgClient.connect()

  try {
    await fn(pgClient)
  } finally {
    await pgClient.end()
  }
}
