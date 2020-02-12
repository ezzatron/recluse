const {EVENT: CHANNEL} = require('../../src/channel.js')
const {appendEvents, readEvents, readEventsByStream} = require('../../src/event.js')
const {waitForNotification} = require('../../src/pg.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')
const {asyncIterableToArray} = require('../helper/async.js')
const {createTestHelper} = require('../helper/pg.js')

describe('appendEvents()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    await initializeSchema(pgHelper.client)
  })

  const typeA = 'stream-type-a'
  const instanceA = 'stream-instance-a'
  const instanceB = 'stream-instance-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: 'a'}
  const eventB = {type: eventTypeB, data: 'b'}
  const eventC = {type: eventTypeA, data: 'c'}
  const eventD = {type: eventTypeB, data: 'd'}

  describe('with a new stream', () => {
    it('should be able to append to the stream', async () => {
      const wasAppended = await pgHelper.inTransaction(
        async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA, eventB]),
      )
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(wasAppended).toBe(true)
      expect(events).toHaveLength(2)
      expect(events[0].event).toEqual(eventA)
      expect(events[1].event).toEqual(eventB)
    })

    it('should be able to append events with null data', async () => {
      const event = {type: eventTypeA, data: null}
      const wasAppended = await pgHelper.inTransaction(
        async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [event]),
      )
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(wasAppended).toBe(true)
      expect(events).toHaveLength(1)
      expect(events[0].event).toEqual({type: eventTypeA, data: null})
    })

    it('should be able to append events with undefined data', async () => {
      const event = {type: eventTypeA}
      const wasAppended = await pgHelper.inTransaction(async () => {
        return appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [event])
      })
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(wasAppended).toBe(true)
      expect(events).toHaveLength(1)
      expect(events[0].event).toEqual({type: eventTypeA, data: undefined})
    })
  })

  describe('with an existing stream', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(
        async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA, eventB]),
      )
    })

    it('should be able to append to the stream', async () => {
      const wasAppended = await pgHelper.inTransaction(
        async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 2, [eventC, eventD]),
      )
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 2))

      expect(wasAppended).toBe(true)
      expect(events).toHaveLength(2)
      expect(events[0].event).toEqual(eventC)
      expect(events[1].event).toEqual(eventD)
    })

    it('should fail if the specified offset is less than the next stream offset', async () => {
      const wasAppended = await pgHelper.inTransaction(
        async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 1, [eventC, eventD]),
      )
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(wasAppended).toBe(false)
      expect(events).toHaveLength(2)
    })

    it('should fail if the specified offset is greater than the next stream offset', async () => {
      const wasAppended = await pgHelper.inTransaction(
        async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 3, [eventC, eventD]),
      )
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(wasAppended).toBe(false)
      expect(events).toHaveLength(2)
    })
  })

  describe('with multiple streams', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA]))
      await pgHelper.inTransaction(async () => appendEvents(serialization, pgHelper.client, typeA, instanceB, 0, [eventB]))
      await pgHelper.inTransaction(async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 1, [eventC]))
      await pgHelper.inTransaction(async () => appendEvents(serialization, pgHelper.client, typeA, instanceB, 1, [eventD]))
    })

    it('should record the global offset', async () => {
      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(4)
      expect(events[0]).toMatchObject({globalOffset: 0})
      expect(events[0].event).toEqual(eventA)
      expect(events[1]).toMatchObject({globalOffset: 1})
      expect(events[1].event).toEqual(eventB)
      expect(events[2]).toMatchObject({globalOffset: 2})
      expect(events[2].event).toEqual(eventC)
      expect(events[3]).toMatchObject({globalOffset: 3})
      expect(events[3].event).toEqual(eventD)
    })
  })

  describe('with multiple clients', () => {
    let secondaryPgClient

    beforeEach(async () => {
      secondaryPgClient = await pgHelper.createClient()
    })

    it('should not allow concurrent writes', async () => {
      // secondaryPgClient is first to start a transaction, but it doesn't automatically acquire a lock
      await secondaryPgClient.query('BEGIN')
      await pgHelper.client.query('BEGIN')

      // this append acquires the lock for pgClient
      await appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA])

      // this append will be started first, but must wait for pgClient's lock to be released to proceed
      const appendB = async () => {
        const result = await appendEvents(serialization, secondaryPgClient, typeA, instanceA, 0, [eventC])

        if (result) {
          await secondaryPgClient.query('COMMIT')
        } else {
          await secondaryPgClient.query('ROLLBACK')
        }

        return result
      }

      // this append will be started second, but can freely proceed since pgClient has the lock
      const appendA = async () => {
        const result = await appendEvents(serialization, pgHelper.client, typeA, instanceA, 1, [eventB])

        if (result) {
          await pgHelper.client.query('COMMIT')
        } else {
          await pgHelper.client.query('ROLLBACK')
        }

        return result
      }

      // these have to be run in parallel
      // awaiting appendB without also awaiting appendA would cause a deadlock
      const [resultB, resultA] = await Promise.all([appendB(), appendA()])

      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(resultA).toBe(true)
      expect(resultB).toBe(false)
      expect(events).toHaveLength(2)
      expect(events[0].event).toEqual(eventA)
      expect(events[1].event).toEqual(eventB)
    })
  })

  describe('with other clients listening for events', () => {
    let secondaryPgClient, waitForEvent

    beforeEach(async () => {
      secondaryPgClient = await pgHelper.createClient()
      await secondaryPgClient.query(`LISTEN ${CHANNEL}`)
      waitForEvent = waitForNotification(secondaryPgClient, CHANNEL)
    })

    it('should notify listening clients when appending events', async () => {
      const [notification] = await Promise.all([
        waitForEvent,
        pgHelper.inTransaction(async () => appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA])),
      ])

      expect(notification.channel).toBe(CHANNEL)
    })
  })
})
