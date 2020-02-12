const {appendEvents, readEventsContinuously} = require('../../src/event.js')
const {consumeAsyncIterable} = require('../helper/async.js')
const {createTestHelper, TIME_PATTERN} = require('../helper/pg.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')

describe('readEventsContinuously()', () => {
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

  describe('with no events', () => {
    it('should support cancellation', async () => {
      expect(await readEventsContinuously(serialization, pgHelper.client).cancel()).toBeUndefined()
    })
  })

  describe('with a non-empty stream', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA, eventB])
    })

    it('should return the correct events for offset 0', async () => {
      const expectedWrappers = [
        {globalOffset: 0, time: TIME_PATTERN},
        {globalOffset: 1, time: TIME_PATTERN},
      ]
      const expected = [
        eventA,
        eventB,
      ]

      await consumeAsyncIterable(
        readEventsContinuously(serialization, pgHelper.client),
        expected.length,
        events => events.cancel(),
        wrapper => {
          expect(wrapper).toMatchObject(expectedWrappers.shift())
          expect(wrapper.event).toEqual(expected.shift())
        },
      )
    })

    it('should return the correct events for positive offsets that exist', async () => {
      const expectedWrappers = [
        {globalOffset: 1, time: TIME_PATTERN},
      ]
      const expected = [
        eventB,
      ]

      await consumeAsyncIterable(
        readEventsContinuously(serialization, pgHelper.client, {start: 1}),
        expected.length,
        events => events.cancel(),
        wrapper => {
          expect(wrapper).toMatchObject(expectedWrappers.shift())
          expect(wrapper.event).toEqual(expected.shift())
        },
      )
    })
  })

  describe('with multiple non-empty streams', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, typeA, instanceA, 0, [eventA, eventB])
      await appendEvents(serialization, pgHelper.client, typeA, instanceB, 0, [eventC, eventD])
    })

    it('should return events for all streams', async () => {
      const expected = [eventA, eventB, eventC, eventD]

      await consumeAsyncIterable(
        readEventsContinuously(serialization, pgHelper.client),
        expected.length,
        events => events.cancel(),
        ({event}) => expect(event).toEqual(expected.shift()),
      )
    })
  })
})
