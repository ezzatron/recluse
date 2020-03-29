const {createContext} = require('../../../src/async.js')
const {COMMAND: CHANNEL} = require('../../../src/channel.js')
const {executeCommands, readCommands} = require('../../../src/command.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('executeCommands()', () => {
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}
  const commandC = {type: commandTypeA, data: 'c'}
  const commandD = {type: commandTypeB, data: 'd'}

  let cancel, context, logger, restore

  const pgHelper = createTestHelper({
    async beforeEach () {
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      restore = configure()
      pgHelper.trackSchemas('recluse')
      await initializeSchema(context, logger, pgHelper.pool)
    },

    async afterEach () {
      restore()
      await cancel()
    },
  })

  describe('with no commands', () => {
    it('should be able to record commands', async () => {
      await pgHelper.inTransaction(
        client => executeCommands(context, logger, client, serialization, sourceA, [commandA, commandB]),
      )

      const commands = []
      await readCommands(context, logger, pgHelper.pool, serialization, 0, ({command}) => {
        commands.push(command)

        return true
      })

      expect(commands).toEqual([commandA, commandB])
    })

    it('should be able to record commands with null data', async () => {
      const command = {type: commandTypeA, data: null}
      await pgHelper.inTransaction(
        client => executeCommands(context, logger, client, serialization, sourceA, [command]),
      )

      const commands = []
      await readCommands(context, logger, pgHelper.pool, serialization, 0, ({command}) => {
        commands.push(command)

        return true
      })

      expect(commands).toEqual([command])
    })

    it('should be able to record commands with undefined data', async () => {
      const command = {type: commandTypeA}
      await pgHelper.inTransaction(
        client => executeCommands(context, logger, client, serialization, sourceA, [command]),
      )

      const commands = []
      await readCommands(context, logger, pgHelper.pool, serialization, 0, ({command}) => {
        commands.push(command)

        return true
      })

      expect(commands).toEqual([command])
    })
  })

  describe('with existing commands', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(
        client => executeCommands(context, logger, client, serialization, sourceA, [commandA, commandB]),
      )
    })

    it('should be able to record commands', async () => {
      await pgHelper.inTransaction(
        client => executeCommands(context, logger, client, serialization, sourceA, [commandC, commandD]),
      )

      const commands = []
      await readCommands(context, logger, pgHelper.pool, serialization, 2, ({command}) => {
        commands.push(command)

        return true
      })

      expect(commands).toEqual([commandC, commandD])
    })
  })

  describe('with other clients listening for commands', () => {
    let listenClient

    beforeEach(async () => {
      listenClient = await pgHelper.pool.connect()
      await listenClient.query(`LISTEN ${CHANNEL}`)
    })

    afterEach(() => {
      listenClient.release()
    })

    it('should notify listening clients when recording commands', async () => {
      let resolveNotified
      const notified = new Promise(resolve => { resolveNotified = resolve })
      listenClient.once('notification', ({channel}) => { resolveNotified(channel) })

      const task = Promise.all([
        notified,
        pgHelper.inTransaction(client => executeCommands(context, logger, client, serialization, sourceA, [commandA])),
      ])

      await expect(task).resolves.toEqual([CHANNEL, undefined])
    })
  })
})
