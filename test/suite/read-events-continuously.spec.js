const {expect} = require('chai')
const {consumeAsyncIterable, pgSpec} = require('../helper.js')

const {appendEvents, readEventsContinuously} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')

describe('readEventsContinuously()', pgSpec(function () {
  const typeA = 'stream-type-a'
  const nameA = 'stream-name-a'
  const nameB = 'stream-name-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventDataA = Buffer.from('a')
  const eventDataB = Buffer.from('b')
  const eventA = {type: eventTypeA, data: eventDataA}
  const eventB = {type: eventTypeB, data: eventDataB}
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
      await appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB])
    })

    it('should return the correct events for offset 0', async function () {
      const expected = [
        {
          global_offset: '0',
          type: eventTypeA,
          data: eventDataA,
          time: Date,
        },
        {
          global_offset: '1',
          type: eventTypeB,
          data: eventDataB,
          time: Date,
        },
      ]

      await consumeAsyncIterable(
        readEventsContinuously(this.pgClient),
        expected.length,
        events => events.cancel(),
        event => expect(event).to.have.fields(expected.shift())
      )
    })

    it('should return the correct events for positive offsets that exist', async function () {
      const expected = [
        {
          global_offset: '1',
          type: eventTypeB,
          data: eventDataB,
          time: Date,
        },
      ]

      await consumeAsyncIterable(
        readEventsContinuously(this.pgClient, {offset: 1}),
        expected.length,
        events => events.cancel(),
        event => expect(event).to.have.fields(expected.shift())
      )
    })
  })

  context('with multiple non-empty streams', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB])
      await appendEvents(this.pgClient, typeA, nameB, 0, [eventC, eventD])
    })

    it('should return events for all streams', async function () {
      const expected = [eventA, eventB, eventC, eventD]

      await consumeAsyncIterable(
        readEventsContinuously(this.pgClient),
        expected.length,
        events => events.cancel(),
        event => expect(event).to.have.fields(expected.shift())
      )
    })
  })
}))
