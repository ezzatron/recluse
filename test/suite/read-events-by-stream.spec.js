const {appendEvents, readEventsByStream} = require('../../src/event.js')
const {asyncIterableToArray, consumeAsyncIterable} = require('../helper/async.js')
const {createTestHelper, TIME_PATTERN} = require('../helper/pg.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')

describe('readEventsByStream()', () => {
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

  describe('with an empty stream', () => {
    it('should return an empty result for start offset 0', async () => {
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(events).toHaveLength(0)
    })

    it('should return an empty result for start offset 0 with a positive end offset', async () => {
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 0, 111))

      expect(events).toHaveLength(0)
    })

    it('should return an empty result for positive start offsets', async () => {
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 111))

      expect(events).toHaveLength(0)
    })

    it('should return an empty result for positive start and end offsets', async () => {
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 111, 222))

      expect(events).toHaveLength(0)
    })

    it('should support cancellation', async () => {
      expect(await readEventsByStream(serialization, pgHelper.client, typeA, instanceA).cancel()).toBeUndefined()
    })

    it('should require a valid stream type', () => {
      expect(() => readEventsByStream(serialization, pgHelper.client)).toThrow('Invalid stream type')
    })

    it('should require a valid stream instance', () => {
      expect(() => readEventsByStream(serialization, pgHelper.client, typeA)).toThrow('Invalid stream instance')
    })
  })

  describe('with a non-empty stream', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA, eventB, eventC])
    })

    it('should return the correct events for start offset 0', async () => {
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(events).toHaveLength(3)
      expect(events[0]).toMatchObject({streamId: 0, streamOffset: 0, time: TIME_PATTERN})
      expect(events[0].event).toEqual(eventA)
      expect(events[1]).toMatchObject({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[1].event).toEqual(eventB)
      expect(events[2]).toMatchObject({streamId: 0, streamOffset: 2, time: TIME_PATTERN})
      expect(events[2].event).toEqual(eventC)
    })

    it('should return the correct events for start offset 0 with a positive end offset', async () => {
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 0, 2))

      expect(events).toHaveLength(2)
      expect(events[0]).toMatchObject({streamId: 0, streamOffset: 0, time: TIME_PATTERN})
      expect(events[0].event).toEqual(eventA)
      expect(events[1]).toMatchObject({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[1].event).toEqual(eventB)
    })

    it('should return the correct events for positive start offsets that exist', async () => {
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 1))

      expect(events).toHaveLength(2)
      expect(events[0]).toMatchObject({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[0].event).toEqual(eventB)
      expect(events[1]).toMatchObject({streamId: 0, streamOffset: 2, time: TIME_PATTERN})
      expect(events[1].event).toEqual(eventC)
    })

    it('should return the correct events for positive start and end offsets that exist', async () => {
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 1, 2))

      expect(events).toHaveLength(1)
      expect(events[0]).toMatchObject({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[0].event).toEqual(eventB)
    })

    it('should return an empty result for positive start offsets that do not exist', async () => {
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 111))

      expect(events).toHaveLength(0)
    })

    it('should return an empty result for positive start and end offsets that do not exist', async () => {
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA, 111, 222))

      expect(events).toHaveLength(0)
    })

    it('should support cancellation', async () => {
      await consumeAsyncIterable(
        readEventsByStream(serialization, pgHelper.client, typeA, instanceA),
        1,
        events => events.cancel(),
        async event => expect(event).toBeDefined(),
      )
    })
  })

  describe('with multiple non-empty streams', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA, eventB])
      await appendEvents(serialization, pgHelper.client, typeA, instanceB, 0, [eventC, eventD])
    })

    it('should only return the events for the requested stream', async () => {
      const [events] = await asyncIterableToArray(readEventsByStream(serialization, pgHelper.client, typeA, instanceA))

      expect(events).toHaveLength(2)
      expect(events[0].event).toEqual(eventA)
      expect(events[1].event).toEqual(eventB)
    })
  })
})
