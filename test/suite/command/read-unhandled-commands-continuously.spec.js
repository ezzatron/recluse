const {createContext} = require('../../../src/async.js')
const {executeCommands, readUnhandledCommandsContinuously} = require('../../../src/command.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper, TIME_PATTERN} = require('../../helper/pg.js')

describe('readUnhandledCommandsContinuously()', () => {
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}

  let cancel, client, context, logger, restore

  const pgHelper = createTestHelper({
    async beforeEach () {
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      restore = configure()
      pgHelper.trackSchemas('recluse')
      await initializeSchema(context, logger, pgHelper.pool)

      client = await pgHelper.pool.connect()
    },

    afterEach () {
      client && client.release(true)
      restore && restore()
      cancel && cancel()
    },
  })

  describe('with only unhandled commands', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await executeCommands(context, logger, client, serialization, sourceA, [commandA, commandB])
      })
    })

    it('should return all commands', async () => {
      const wrappers = []
      await readUnhandledCommandsContinuously(context, logger, client, serialization, {}, wrapper => {
        wrappers.push(wrapper)

        return wrappers.length < 2
      })

      expect(wrappers).toEqual([
        {id: 0, source: sourceA, executedAt: expect.stringMatching(TIME_PATTERN), handledAt: null, command: commandA},
        {id: 1, source: sourceA, executedAt: expect.stringMatching(TIME_PATTERN), handledAt: null, command: commandB},
      ])
    })
  })

  describe('with some handled commands', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await executeCommands(context, logger, client, serialization, sourceA, [commandA, commandB])
        await client.query('UPDATE recluse.command SET handled_at = now() WHERE id = 0')
      })
    })

    it('should return only the unhandled commands', async () => {
      const wrappers = []
      await readUnhandledCommandsContinuously(context, logger, client, serialization, {}, wrapper => {
        wrappers.push(wrapper)

        return false
      })

      expect(wrappers).toEqual([
        {id: 1, source: sourceA, executedAt: expect.stringMatching(TIME_PATTERN), handledAt: null, command: commandB},
      ])
    })
  })
})
