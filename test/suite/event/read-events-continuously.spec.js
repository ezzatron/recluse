const {createContext} = require('../../../src/async.js')
const {appendEvents, readEventsContinuously} = require('../../../src/event.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper, TIME_PATTERN} = require('../../helper/pg.js')

describe('readEventsContinuously()', () => {
  const typeA = 'stream-type-a'
  const instanceA = 'stream-instance-a'
  const instanceB = 'stream-instance-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: 'a'}
  const eventB = {type: eventTypeB, data: 'b'}
  const eventC = {type: eventTypeA, data: 'c'}
  const eventD = {type: eventTypeB, data: 'd'}

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

  describe('with a non-empty stream', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [eventA, eventB])
      })
    })

    it('should return the correct events for offset 0', async () => {
      const wrappers = []
      await readEventsContinuously(context, logger, pgHelper.pool, serialization, {}, wrapper => {
        wrappers.push(wrapper)

        return wrapper.globalOffset < 1
      })

      expect(wrappers).toEqual([
        {globalOffset: 0, streamId: 0, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventA},
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
      ])
    })

    it('should return the correct events for positive offsets that exist', async () => {
      const wrappers = []
      await readEventsContinuously(context, logger, pgHelper.pool, serialization, {start: 1}, wrapper => {
        wrappers.push(wrapper)

        return false
      })

      expect(wrappers).toEqual([
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
      ])
    })
  })

  describe('with multiple non-empty streams', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [eventA, eventB])
        await appendEvents(context, logger, client, serialization, typeA, instanceB, 0, [eventC, eventD])
      })
    })

    it('should return events for all streams', async () => {
      const wrappers = []
      await readEventsContinuously(context, logger, pgHelper.pool, serialization, {}, wrapper => {
        wrappers.push(wrapper)

        return wrapper.globalOffset < 3
      })

      expect(wrappers).toEqual([
        {globalOffset: 0, streamId: 0, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventA},
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
        {globalOffset: 2, streamId: 1, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventC},
        {globalOffset: 3, streamId: 1, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventD},
      ])
    })
  })
})
