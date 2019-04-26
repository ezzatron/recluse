const {expect} = require('chai')

const {asyncIterableToArray, consumeAsyncIterable, pgSpec, TIME_PATTERN} = require('../helper.js')

const {appendEvents, readEventsByStream} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')

describe('readEventsByStream()', pgSpec(function () {
  const typeA = 'stream-type-a'
  const instanceA = 'stream-instance-a'
  const instanceB = 'stream-instance-b'
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

  context('with an empty stream', function () {
    it('should return an empty result for offset 0', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, instanceA))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for positive offsets', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, instanceA, 111))

      expect(events).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await readEventsByStream(this.pgClient, instanceA).cancel()
    })

    it('should require a valid stream instance', function () {
      expect(() => readEventsByStream(this.pgClient)).to.throw('Invalid stream instance')
    })
  })

  context('with a non-empty stream', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, instanceA, 0, [eventA, eventB])
    })

    it('should return the correct events for offset 0', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, instanceA))

      expect(events).to.have.length(2)
      expect(events[0]).to.have.fields({
        streamId: 0,
        streamOffset: 0,
        time: TIME_PATTERN,
      })
      expect(events[0].event).to.have.fields({
        type: eventTypeA,
        data: eventDataA,
      })
      expect(events[1]).to.have.fields({
        streamId: 0,
        streamOffset: 1,
        time: TIME_PATTERN,
      })
      expect(events[1].event).to.have.fields({
        type: eventTypeB,
        data: eventDataB,
      })
    })

    it('should return the correct events for positive offsets that exist', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, instanceA, 1))

      expect(events).to.have.length(1)
      expect(events[0]).to.have.fields({
        streamId: 0,
        streamOffset: 1,
        time: TIME_PATTERN,
      })
      expect(events[0].event).to.have.fields({
        type: eventTypeB,
        data: eventDataB,
      })
    })

    it('should return an empty result for positive offsets that do not exist', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, instanceA, 111))

      expect(events).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await consumeAsyncIterable(
        readEventsByStream(this.pgClient, instanceA),
        1,
        events => events.cancel(),
        async event => expect(event).to.exist()
      )
    })
  })

  context('with multiple non-empty streams', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, instanceA, 0, [eventA, eventB])
      await appendEvents(this.pgClient, typeA, instanceB, 0, [eventC, eventD])
    })

    it('should only return the events for the requested stream', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, instanceA))

      expect(events).to.have.length(2)
      expect(events[0].event).to.have.fields(eventA)
      expect(events[1].event).to.have.fields(eventB)
    })
  })
}))
