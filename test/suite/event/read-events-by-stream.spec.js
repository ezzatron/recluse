const {createContext} = require('../../../src/async.js')
const {appendEvents, readEventsByStream} = require('../../../src/event.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper, TIME_PATTERN} = require('../../helper/pg.js')

describe('readEventsByStream()', () => {
  const typeA = 'stream-type-a'
  const instanceA = 'stream-instance-a'
  const instanceB = 'stream-instance-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: 'a'}
  const eventB = {type: eventTypeB, data: 'b'}
  const eventC = {type: eventTypeA, data: 'c'}
  const eventD = {type: eventTypeB, data: 'd'}

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

    async afterEach () {
      client && client.release(true)
      restore()
      await cancel()
    },
  })

  describe('with an empty stream', () => {
    it('should return an empty result for start offset 0 with no end offset', async () => {
      const wrappers = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for start offset 0 with a positive end offset', async () => {
      const wrappers = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, 111, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for positive start offsets with no end offset', async () => {
      const wrappers = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 111, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for positive start and end offsets', async () => {
      const wrappers = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 111, 222, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should require a valid stream type', async () => {
      const task = readEventsByStream(context, logger, client, serialization, null, instanceA, 0, Infinity, () => {})

      await expect(task).rejects.toThrow('Invalid stream type')
    })

    it('should require a valid stream instance', async () => {
      const task = readEventsByStream(context, logger, client, serialization, typeA, null, 0, Infinity, () => {})

      await expect(task).rejects.toThrow('Invalid stream instance')
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
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, wrapper => {
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
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, 2, wrapper => {
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
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 1, Infinity, wrapper => {
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
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 1, 2, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
      ])
    })

    it('should return an empty result for positive start offsets that do not exist, with no end offset', async () => {
      const wrappers = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 111, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should return an empty result for positive start and end offsets that do not exist', async () => {
      const wrappers = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 111, 222, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([])
    })

    it('should support partially consuming events', async () => {
      const wrappers = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, wrapper => {
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

    it('should only return the events for the requested stream', async () => {
      const wrappersA = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, wrapper => {
        wrappersA.push(wrapper)

        return true
      })

      expect(wrappersA).toEqual([
        {globalOffset: 0, streamId: 0, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventA},
        {globalOffset: 1, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventB},
      ])
    })
  })
})
