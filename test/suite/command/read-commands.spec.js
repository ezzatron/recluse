const {createContext} = require('../../../src/async.js')
const {executeCommands, readCommands} = require('../../../src/command.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper, TIME_PATTERN} = require('../../helper/pg.js')

describe('readCommands()', () => {
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}

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
    it('should return an empty result for ID 0', async () => {
      const wrappers = []
      await readCommands(context, logger, pgHelper.pool, serialization, 0, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for positive IDs', async () => {
      const wrappers = []
      await readCommands(context, logger, pgHelper.pool, serialization, 111, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })
  })

  describe('with existing commands', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(
        client => executeCommands(context, logger, client, serialization, sourceA, [commandA, commandB]),
      )
    })

    it('should return the correct commands for ID 0', async () => {
      const wrappers = []
      await readCommands(context, logger, pgHelper.pool, serialization, 0, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {id: 0, source: sourceA, executedAt: expect.stringMatching(TIME_PATTERN), handledAt: null, command: commandA},
        {id: 1, source: sourceA, executedAt: expect.stringMatching(TIME_PATTERN), handledAt: null, command: commandB},
      ])
    })

    it('should return the correct commands for positive IDs that exist', async () => {
      const wrappers = []
      await readCommands(context, logger, pgHelper.pool, serialization, 1, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {id: 1, source: sourceA, executedAt: expect.stringMatching(TIME_PATTERN), handledAt: null, command: commandB},
      ])
    })

    it('should return an empty result for positive IDs that do not exist', async () => {
      const wrappers = []
      await readCommands(context, logger, pgHelper.pool, serialization, 111, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should support partially consuming commands', async () => {
      const wrappers = []
      await readCommands(context, logger, pgHelper.pool, serialization, 0, wrapper => {
        wrappers.push(wrapper)

        return false
      })

      expect(wrappers).toEqual([
        {id: 0, source: sourceA, executedAt: expect.stringMatching(TIME_PATTERN), handledAt: null, command: commandA},
      ])
    })
  })
})
