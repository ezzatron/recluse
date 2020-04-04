const {createContext} = require('../../../src/async.js')
const {appendEvents, readEvents} = require('../../../src/event.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper, TIME_PATTERN} = require('../../helper/pg.js')

describe('readEvents()', () => {
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

  describe('with no events', () => {
    it('should return an empty result for start offset 0 with no end offset', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 0, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for start offset 0 with a positive end offset', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 0, 111, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for positive start offsets with no end offset', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 111, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for positive start and end offsets', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 111, 222, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })
  })

  describe('with a non-empty stream', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [eventA, eventB, eventC])
      })
    })

    it('should return the correct events for start offset 0 with no end offset', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 0, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {globalOffset: 0, streamId: 0, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventA},
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
        {globalOffset: 2, streamId: 0, streamOffset: 2, time: expect.stringMatching(TIME_PATTERN), event: eventC},
      ])
    })

    it('should return the correct events for start offset 0 with a positive end offset', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 0, 2, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {globalOffset: 0, streamId: 0, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventA},
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
      ])
    })

    it('should return the correct events for positive start offsets that exist, with no end offset', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 1, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
        {globalOffset: 2, streamId: 0, streamOffset: 2, time: expect.stringMatching(TIME_PATTERN), event: eventC},
      ])
    })

    it('should return the correct events for positive start and end offsets that exist', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 1, 2, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
      ])
    })

    it('should return an empty result for positive start offsets that do not exist, with no end offset', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 111, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for positive start and end offsets that do not exist', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 111, 222, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should support partially consuming events', async () => {
      const wrappers = []
      await readEvents(context, logger, pgHelper.pool, serialization, 0, Infinity, wrapper => {
        wrappers.push(wrapper)

        return false
      })

      expect(wrappers).toEqual([
        {globalOffset: 0, streamId: 0, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventA},
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
      await readEvents(context, logger, pgHelper.pool, serialization, 0, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
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
