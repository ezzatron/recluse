const {expect} = require('chai')

const {asyncIterableToArray, consumeAsyncIterable, pgSpec, TIME_PATTERN} = require('../helper.js')

const {appendEvents, readEvents} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')

describe('readEvents()', pgSpec(function () {
  const typeA = 'stream-type-a'
  const instanceA = 'stream-instance-a'
  const instanceB = 'stream-instance-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: 'a'}
  const eventB = {type: eventTypeB, data: 'b'}
  const eventC = {type: eventTypeA, data: 'c'}
  const eventD = {type: eventTypeB, data: 'd'}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with no events', function () {
    it('should return an empty result for start offset 0', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for start offset 0 with a positive end offset', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 0, 111))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for positive start offsets', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 111))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for positive start and end offsets', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 111, 222))

      expect(events).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await readEvents(serialization, this.pgClient).cancel()
    })
  })

  context('with a non-empty stream', function () {
    beforeEach(async function () {
      await appendEvents(serialization, this.pgClient, typeA, instanceA, 0, [eventA, eventB, eventC])
    })

    it('should return the correct events for start offset 0', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient))

      expect(events).to.have.length(3)
      expect(events[0]).to.have.fields({globalOffset: 0, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventA)
      expect(events[1]).to.have.fields({globalOffset: 1, time: TIME_PATTERN})
      expect(events[1].event).to.deep.equal(eventB)
      expect(events[2]).to.have.fields({globalOffset: 2, time: TIME_PATTERN})
      expect(events[2].event).to.deep.equal(eventC)
    })

    it('should return the correct events for start offset 0 with a positive end offset', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 0, 2))

      expect(events).to.have.length(2)
      expect(events[0]).to.have.fields({globalOffset: 0, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventA)
      expect(events[1]).to.have.fields({globalOffset: 1, time: TIME_PATTERN})
      expect(events[1].event).to.deep.equal(eventB)
    })

    it('should return the correct events for positive start offsets that exist', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 1))

      expect(events).to.have.length(2)
      expect(events[0]).to.have.fields({globalOffset: 1, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventB)
      expect(events[1]).to.have.fields({globalOffset: 2, time: TIME_PATTERN})
      expect(events[1].event).to.deep.equal(eventC)
    })

    it('should return the correct events for positive start and end offsets that exist', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 1, 2))

      expect(events).to.have.length(1)
      expect(events[0]).to.have.fields({globalOffset: 1, time: TIME_PATTERN})
      expect(events[0].event).to.deep.equal(eventB)
    })

    it('should return an empty result for positive start offsets that do not exist', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 111))

      expect(events).to.have.length(0)
    })

    it('should return an empty result for positive start and end offsets that do not exist', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient, 111, 222))

      expect(events).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await consumeAsyncIterable(
        readEvents(serialization, this.pgClient),
        1,
        events => events.cancel(),
        event => expect(event).to.exist(),
      )
    })
  })

  context('with multiple non-empty streams', function () {
    beforeEach(async function () {
      await appendEvents(serialization, this.pgClient, typeA, instanceA, 0, [eventA, eventB])
      await appendEvents(serialization, this.pgClient, typeA, instanceB, 0, [eventC, eventD])
    })

    it('should return events for all streams', async function () {
      const [events] = await asyncIterableToArray(readEvents(serialization, this.pgClient))

      expect(events).to.have.length(4)
      expect(events[0].event).to.deep.equal(eventA)
      expect(events[1].event).to.deep.equal(eventB)
      expect(events[2].event).to.deep.equal(eventC)
      expect(events[3].event).to.deep.equal(eventD)
    })
  })
}))
