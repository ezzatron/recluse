const {expect} = require('chai')
const {asyncIterableToArray, pgSpec} = require('../helper.js')

const {appendEvents, initializeSchema, readEvents} = require('../../src/index.js')

describe('readEvents()', pgSpec(function () {
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
    it('should return an empty result for offset 0', async function () {
      const [actual] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(actual).to.have.length(0)
    })

    it('should return an empty result for positive offsets', async function () {
      const [actual] = await asyncIterableToArray(readEvents(this.pgClient, 111))

      expect(actual).to.have.length(0)
    })
  })

  context('with a non-empty stream', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB])
    })

    it('should return the correct events for offset 0', async function () {
      const [actual] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(actual).to.have.length(2)
      expect(actual[0]).to.have.fields({
        global_offset: '0',
        type: eventTypeA,
        data: eventDataA,
        time: Date,
      })
      expect(actual[1]).to.have.fields({
        global_offset: '1',
        type: eventTypeB,
        data: eventDataB,
        time: Date,
      })
    })

    it('should return the correct events for positive offsets that exist', async function () {
      const [actual] = await asyncIterableToArray(readEvents(this.pgClient, 1))

      expect(actual).to.have.length(1)
      expect(actual[0]).to.have.fields({
        global_offset: '1',
        type: eventTypeB,
        data: eventDataB,
        time: Date,
      })
    })

    it('should return an empty result for positive offsets that do not exist', async function () {
      const [actual] = await asyncIterableToArray(readEvents(this.pgClient, 111))

      expect(actual).to.have.length(0)
    })
  })

  context('with multiple non-empty streams', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB])
      await appendEvents(this.pgClient, typeA, nameB, 0, [eventC, eventD])
    })

    it('should return events for all streams', async function () {
      const [actual] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(actual).to.have.length(4)
      expect(actual[0]).to.have.fields(eventA)
      expect(actual[1]).to.have.fields(eventB)
      expect(actual[2]).to.have.fields(eventC)
      expect(actual[3]).to.have.fields(eventD)
    })
  })
}))