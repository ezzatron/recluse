const {basename} = require('path')
const {Client} = require('pg')

const {executeCommands: executeCommandsRaw} = require('../../../src/command.js')
const {serialization} = require('../../../src/serialization/json.js')

module.exports = {
  executeCommands,
}

function executeCommands (...commands) {
  async function execute () {
    const binaryName = basename(process.argv[1])
    const [,, accountId, amountString] = process.argv
    const amount = parseInt(amountString)

    if (!accountId) throw new Error('Invalid account ID')
    if (!amount) throw new Error('Invalid amount')

    const pgClient = new Client()
    await pgClient.connect()

    try {
      await executeCommandsRaw(serialization, pgClient, `cli.${binaryName}`, commands)
    } finally {
      await pgClient.end()
    }
  }

  execute().catch(error => {
    console.error(error.stack)

    process.exit(1)
  })
}
