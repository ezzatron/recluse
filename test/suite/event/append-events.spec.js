const {createContext} = require('../../../src/async.js')
const {EVENT: CHANNEL} = require('../../../src/channel.js')
const {appendEvents, readEvents, readEventsByStream} = require('../../../src/event.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper, TIME_PATTERN} = require('../../helper/pg.js')

describe('appendEvents()', () => {
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
      client.release(true)
      restore()
      await cancel()
    },
  })

  describe('with a new stream', () => {
    it('should be able to append to the stream', async () => {
      const wasAppended =
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [eventA, eventB])

      const events = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, ({event}) => {
        events.push(event)

        return true
      })

      expect(wasAppended).toBe(true)
      expect(events).toEqual([eventA, eventB])
    })

    it('should be able to append events with null data', async () => {
      const event = {type: eventTypeA, data: null}
      const wasAppended = await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [event])

      const events = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, ({event}) => {
        events.push(event)

        return true
      })

      expect(wasAppended).toBe(true)
      expect(events).toEqual([event])
    })

    it('should be able to append events with undefined data', async () => {
      const event = {type: eventTypeA}
      const wasAppended = await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [event])

      const events = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, ({event}) => {
        events.push(event)

        return true
      })

      expect(wasAppended).toBe(true)
      expect(events).toEqual([event])
    })
  })

  describe('with an existing stream', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [eventA, eventB])
      })
    })

    it('should be able to append to the stream', async () => {
      const wasAppended =
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 2, [eventC, eventD])

      const events = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, ({event}) => {
        events.push(event)

        return true
      })

      expect(wasAppended).toBe(true)
      expect(events).toEqual([eventA, eventB, eventC, eventD])
    })

    it('should fail if the specified offset is less than the next stream offset', async () => {
      const wasAppended =
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 1, [eventC, eventD])

      const events = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, ({event}) => {
        events.push(event)

        return true
      })

      expect(wasAppended).toBe(false)
      expect(events).toEqual([eventA, eventB])
    })

    it('should fail if the specified offset is greater than the next stream offset', async () => {
      const wasAppended =
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 3, [eventC, eventD])

      const events = []
      await readEventsByStream(context, logger, client, serialization, typeA, instanceA, 0, Infinity, ({event}) => {
        events.push(event)

        return true
      })

      expect(wasAppended).toBe(false)
      expect(events).toEqual([eventA, eventB])
    })
  })

  describe('with multiple streams', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [eventA])
        await appendEvents(context, logger, client, serialization, typeA, instanceB, 0, [eventB])
        await appendEvents(context, logger, client, serialization, typeA, instanceA, 1, [eventC])
        await appendEvents(context, logger, client, serialization, typeA, instanceB, 1, [eventD])
      })
    })

    it('should record the global offset', async () => {
      const wrappers = []
      await readEvents(context, logger, client, serialization, 0, Infinity, wrapper => {
        wrappers.push(wrapper)

        return true
      })

      expect(wrappers).toEqual([
        {globalOffset: 0, streamId: 0, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventA},
        {globalOffset: 1, streamId: 1, streamOffset: 0, time: expect.stringMatching(TIME_PATTERN), event: eventB},
        {globalOffset: 2, streamId: 0, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventC},
        {globalOffset: 3, streamId: 1, streamOffset: 1, time: expect.stringMatching(TIME_PATTERN), event: eventD},
      ])
    })
  })

  describe('with multiple clients', () => {
    let clientA, clientB

    beforeEach(async () => {
      clientA = await pgHelper.pool.connect()
      clientB = await pgHelper.pool.connect()
    })

    afterEach(() => {
      clientA.release(true)
      clientB.release(true)
    })

    it('should not allow concurrent writes', async () => {
      // clientB is first to start a transaction, but it doesn't automatically acquire a lock
      await clientB.query('BEGIN')
      await clientA.query('BEGIN')

      // this append acquires the lock for clientA
      await appendEvents(context, logger, clientA, serialization, typeA, instanceA, 0, [eventA])

      // this append will be started first, but must wait for clientA's lock to be released to proceed
      const appendB = async () => {
        const result = await appendEvents(context, logger, clientB, serialization, typeA, instanceA, 0, [eventC])

        if (result) {
          await clientB.query('COMMIT')
        } else {
          await clientB.query('ROLLBACK')
        }

        return result
      }

      // this append will be started second, but can freely proceed since clientA has the lock
      const appendA = async () => {
        const result = await appendEvents(context, logger, clientA, serialization, typeA, instanceA, 1, [eventB])

        if (result) {
          await clientA.query('COMMIT')
        } else {
          await clientA.query('ROLLBACK')
        }

        return result
      }

      // these have to be run in parallel
      // awaiting appendB without also awaiting appendA would cause a deadlock
      const [resultB, resultA] = await Promise.all([appendB(), appendA()])

      const events = []
      await readEvents(context, logger, client, serialization, 0, Infinity, ({event}) => {
        events.push(event)

        return true
      })

      expect(resultA).toBe(true)
      expect(resultB).toBe(false)
      expect(events).toEqual([eventA, eventB])
    })
  })

  describe('with other clients listening for commands', () => {
    let listenClient

    beforeEach(async () => {
      listenClient = await pgHelper.pool.connect()
      await listenClient.query(`LISTEN ${CHANNEL}`)
    })

    afterEach(() => {
      listenClient.release(true)
    })

    it('should notify listening clients when appending events', async () => {
      let resolveNotified
      const notified = new Promise(resolve => { resolveNotified = resolve })
      listenClient.once('notification', ({channel}) => { resolveNotified(channel) })

      const task = Promise.all([
        notified,
        pgHelper.inTransaction(async client => {
          return appendEvents(context, logger, client, serialization, typeA, instanceA, 0, [eventA])
        }),
      ])

      await expect(task).resolves.toEqual([CHANNEL, true])
    })
  })
})
