const {createContext} = require('../../../src/async.js')
const {appendEvents, readNextStreamOffset} = require('../../../src/event.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('readNextStreamOffset()', () => {
  const type = 'stream-type-a'
  const instance = 'stream-instance-a'
  const event = {type: 'event-type-a', data: 'a'}

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

  describe('with an empty stream', () => {
    it('should return offset 0', async () => {
      expect(await readNextStreamOffset(context, logger, pgHelper.pool, type, instance)).toBe(0)
    })

    it('should require a valid stream type', async () => {
      await expect(readNextStreamOffset(context, logger, pgHelper.pool, null, instance))
        .rejects.toThrow('Invalid stream type')
    })

    it('should require a valid stream instance', async () => {
      await expect(readNextStreamOffset(context, logger, pgHelper.pool, type, null))
        .rejects.toThrow('Invalid stream instance')
    })
  })

  describe('with a non-empty stream', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, type, instance, 0, [event, event])
      })
    })

    it('should return a positive offset', async () => {
      expect(await readNextStreamOffset(context, logger, pgHelper.pool, type, instance)).toBe(2)
    })
  })
})
