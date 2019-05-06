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
  const eventA = {type: eventTypeA, data: Buffer.from('a')}
  const eventB = {type: eventTypeB, data: Buffer.from('b')}
  const eventC = {type: eventTypeA, data: Buffer.from('c')}
  const eventD = {type: eventTypeB, data: Buffer.from('d')}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with an empty stream', function () {
    it('should return an empty result for start offset 0', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for start offset 0 with a positive end offset', async function () {
      const [events] =
        await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 0, 111))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for positive start offsets', async function () {
      const [events] =
        await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 111))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for positive start and end offsets', async function () {
      const [events] =
        await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 111, 222))

      expect(events).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await readEventsByStream(this.pgClient, typeA, instanceA).cancel()
    })

    it('should require a valid stream type', function () {
      expect(() => readEventsByStream(this.pgClient)).to.throw('Invalid stream type')
    })

    it('should require a valid stream instance', function () {
      expect(() => readEventsByStream(this.pgClient, typeA)).to.throw('Invalid stream instance')
    })
  })

  context('with a non-empty stream', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, instanceA, 0, [eventA, eventB, eventC])
    })

    it('should return the correct events for start offset 0', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA))

      expect(events).to.have.length(3)
      expect(events[0]).to.have.fields({streamId: 0, streamOffset: 0, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventA)
      expect(events[1]).to.have.fields({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[1].event).to.deep.equal(eventB)
      expect(events[2]).to.have.fields({streamId: 0, streamOffset: 2, time: TIME_PATTERN})
      expect(events[2].event).to.deep.equal(eventC)
    })

    it('should return the correct events for start offset 0 with a positive end offset', async function () {
      const [events] =
        await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 0, 2))

      expect(events).to.have.length(2)
      expect(events[0]).to.have.fields({streamId: 0, streamOffset: 0, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventA)
      expect(events[1]).to.have.fields({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[1].event).to.deep.equal(eventB)
    })

    it('should return the correct events for positive start offsets that exist', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 1))

      expect(events).to.have.length(2)
      expect(events[0]).to.have.fields({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventB)
      expect(events[1]).to.have.fields({streamId: 0, streamOffset: 2, time: TIME_PATTERN})
      expect(events[1].event).to.deep.equal(eventC)
    })

    it('should return the correct events for positive start and end offsets that exist', async function () {
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 1, 2))

      expect(events).to.have.length(1)
      expect(events[0]).to.have.fields({streamId: 0, streamOffset: 1, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventB)
    })

    it('should return an empty result for positive start offsets that do not exist', async function () {
      const [events] =
        await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 111))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for positive start and end offsets that do not exist', async function () {
      const [events] =
        await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA, 111, 222))

      expect(events).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await consumeAsyncIterable(
        readEventsByStream(this.pgClient, typeA, instanceA),
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
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, typeA, instanceA))

      expect(events).to.have.length(2)
      expect(events[0].event).to.deep.equal(eventA)
      expect(events[1].event).to.deep.equal(eventB)
    })
  })
}))
