const {expect} = require('chai')

const {consumeAsyncIterable, pgSpec, TIME_PATTERN} = require('../helper.js')

const {appendEvents, readEventsContinuously} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')

describe('readEventsContinuously()', pgSpec(function () {
  const typeA = 'stream-type-a'
  const instanceA = 'stream-instance-a'
  const instanceB = 'stream-instance-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: Buffer.from('a')}
  const eventB = {type: eventTypeB, data: Buffer.from('b')}
  const eventC = {type: eventTypeA, data: Buffer.from('c')}
  const eventD = {type: eventTypeB, data: Buffer.from('d')}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with no events', function () {
    it('should support cancellation', async function () {
      await readEventsContinuously(this.pgClient).cancel()
    })
  })

  context('with a non-empty stream', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, instanceA, 0, [eventA, eventB])
    })

    it('should return the correct events for offset 0', async function () {
      const expectedWrappers = [
        {globalOffset: 0, time: TIME_PATTERN},
        {globalOffset: 1, time: TIME_PATTERN},
      ]
      const expected = [
        eventA,
        eventB,
      ]

      await consumeAsyncIterable(
        readEventsContinuously(this.pgClient),
        expected.length,
        events => events.cancel(),
        wrapper => {
          expect(wrapper).to.have.fields(expectedWrappers.shift())
          expect(wrapper.event).to.deep.equal(expected.shift())
        }
      )
    })

    it('should return the correct events for positive offsets that exist', async function () {
      const expectedWrappers = [
        {globalOffset: 1, time: TIME_PATTERN},
      ]
      const expected = [
        eventB,
      ]

      await consumeAsyncIterable(
        readEventsContinuously(this.pgClient, {start: 1}),
        expected.length,
        events => events.cancel(),
        wrapper => {
          expect(wrapper).to.have.fields(expectedWrappers.shift())
          expect(wrapper.event).to.deep.equal(expected.shift())
        }
      )
    })
  })

  context('with multiple non-empty streams', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, instanceA, 0, [eventA, eventB])
      await appendEvents(this.pgClient, typeA, instanceB, 0, [eventC, eventD])
    })

    it('should return events for all streams', async function () {
      const expected = [eventA, eventB, eventC, eventD]

      await consumeAsyncIterable(
        readEventsContinuously(this.pgClient),
        expected.length,
        events => events.cancel(),
        ({event}) => expect(event).to.deep.equal(expected.shift())
      )
    })
  })
}))
