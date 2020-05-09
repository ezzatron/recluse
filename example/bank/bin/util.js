const {basename} = require('path')
const {Client} = require('pg')

const {createContext} = require('../../../src/async.js')
const {executeCommands: executeCommandsRaw} = require('../../../src/command.js')
const {createLoggerFactory} = require('../../../src/logging.js')
const {serialization} = require('../../../src/serialization/json.js')

module.exports = {
  accountIdByName,
  die,
  executeCommands,
  inTransaction,
  runAsync,
}

async function accountIdByName (client, name) {
  const result = await client.query('SELECT id FROM bank.account WHERE name = $1', [name])

  return result.rowCount > 0 ? result.rows[0].id : null
}

function die (message) {
  console.log(message) // eslint-disable-line no-console

  process.exit(1)
}

async function executeCommands (client, ...commands) {
  const binaryName = basename(process.argv[1])

  const createLogger = createLoggerFactory(process.env)
  const logger = createLogger(binaryName)
  const [context] = createContext(logger)

  await executeCommandsRaw(context, logger, client, serialization, `cli.${binaryName}`, commands)
}

async function inTransaction (fn) {
  const client = new Client()
  await client.connect()

  try {
    let result

    await client.query('BEGIN')

    try {
      result = await fn(client)
    } catch (error) {
      await client.query('ROLLBACK')

      throw error
    }

    await client.query('COMMIT')

    return result
  } finally {
    await client.end()
  }
}

function runAsync (fn) {
  fn.catch(error => {
    console.error(error.stack) // eslint-disable-line no-console

    process.exit(1)
  })
}
