const {expect} = require('chai')
const {asyncIterableToArray, pgSpec} = require('../helper.js')

const {appendEvents, readEvents, readEventsByStream} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')
const {waitForNotification} = require('../../src/pg.js')

describe('appendEvents()', pgSpec(function () {
  const typeA = 'stream-type-a'
  const nameA = 'stream-name-a'
  const nameB = 'stream-name-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: Buffer.from('a')}
  const eventB = {type: eventTypeB, data: Buffer.from('b')}
  const eventC = {type: eventTypeA, data: Buffer.from('c')}
  const eventD = {type: eventTypeB, data: Buffer.from('d')}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with a new stream', function () {
    it('should be able to append to the stream', async function () {
      const wasAppended =
        await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB]))
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, nameA))

      expect(wasAppended).to.be.true()
      expect(events).to.have.length(2)
      expect(events[0].event).to.have.fields(eventA)
      expect(events[1].event).to.have.fields(eventB)
    })

    it('should be able to append events with null data', async function () {
      const event = {type: eventTypeA, data: null}
      const wasAppended = await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [event]))
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, nameA))

      expect(wasAppended).to.be.true()
      expect(events).to.have.length(1)
      expect(events[0].event).to.have.fields({type: eventTypeA})
      expect(events[0].event.data).to.be.undefined()
    })

    it('should be able to append events with undefined data', async function () {
      const event = {type: eventTypeA}
      const wasAppended = await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [event]))
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, nameA))

      expect(wasAppended).to.be.true()
      expect(events).to.have.length(1)
      expect(events[0].event).to.have.fields({type: eventTypeA})
      expect(events[0].event.data).to.be.undefined()
    })
  })

  context('with an existing stream', function () {
    beforeEach(async function () {
      await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB]))
    })

    it('should be able to append to the stream', async function () {
      const wasAppended = await this.inTransaction(
        async () => appendEvents(this.pgClient, typeA, nameA, 2, [eventC, eventD])
      )
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, nameA, 2))

      expect(wasAppended).to.be.true()
      expect(events).to.have.length(2)
      expect(events[0].event).to.have.fields(eventC)
      expect(events[1].event).to.have.fields(eventD)
    })

    it('should fail if the specified offset is less than the next stream offset', async function () {
      const wasAppended =
        await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 1, [eventC, eventD]))
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, nameA))

      expect(wasAppended).to.be.false()
      expect(events).to.have.length(2)
    })

    it('should fail if the specified offset is greater than the next stream offset', async function () {
      const wasAppended =
        await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 3, [eventC, eventD]))
      const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, nameA))

      expect(wasAppended).to.be.false()
      expect(events).to.have.length(2)
    })
  })

  context('with multiple streams', function () {
    beforeEach(async function () {
      await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [eventA]))
      await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameB, 0, [eventB]))
      await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 1, [eventC]))
      await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameB, 1, [eventD]))
    })

    it('should record the global offset', async function () {
      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(4)
      expect(events[0]).to.have.fields({globalOffset: 0})
      expect(events[0].event).to.have.fields(eventA)
      expect(events[1]).to.have.fields({globalOffset: 1})
      expect(events[1].event).to.have.fields(eventB)
      expect(events[2]).to.have.fields({globalOffset: 2})
      expect(events[2].event).to.have.fields(eventC)
      expect(events[3]).to.have.fields({globalOffset: 3})
      expect(events[3].event).to.have.fields(eventD)
    })
  })

  context('with multiple clients', function () {
    beforeEach(async function () {
      this.secondaryPgClient = await this.createPgClient()
    })

    it('should not allow concurrent writes', async function () {
      // secondaryPgClient is first to start a transaction, but it doesn't automatically acquire a lock
      await this.secondaryPgClient.query('BEGIN')
      await this.pgClient.query('BEGIN')

      // this append acquires the lock for pgClient
      await appendEvents(this.pgClient, typeA, nameA, 0, [eventA])

      // this append will be started first, but must wait for pgClient's lock to be released to proceed
      const appendB = async () => {
        const result = await appendEvents(this.secondaryPgClient, typeA, nameA, 0, [eventC])

        if (result) {
          await this.secondaryPgClient.query('COMMIT')
        } else {
          await this.secondaryPgClient.query('ROLLBACK')
        }

        return result
      }

      // this append will be started second, but can freely proceed since pgClient has the lock
      const appendA = async () => {
        const result = await appendEvents(this.pgClient, typeA, nameA, 1, [eventB])

        if (result) {
          await this.pgClient.query('COMMIT')
        } else {
          await this.pgClient.query('ROLLBACK')
        }

        return result
      }

      // these have to be run in parallel
      // awaiting appendB without also awaiting appendA would cause a deadlock
      const [resultB, resultA] = await Promise.all([appendB(), appendA()])

      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(resultA).to.be.true()
      expect(resultB).to.be.false()
      expect(events).to.have.length(2)
      expect(events[0].event).to.have.fields(eventA)
      expect(events[1].event).to.have.fields(eventB)
    })
  })

  context('with other clients listening for events', function () {
    beforeEach(async function () {
      this.secondaryPgClient = await this.createPgClient()
      await this.secondaryPgClient.query('LISTEN recluse_event')
      this.waitForEvent = waitForNotification(this.secondaryPgClient, 'recluse_event')
    })

    it('should notify listening clients when appending events', async function () {
      const [notification] = await Promise.all([
        this.waitForEvent,
        this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [eventA])),
      ])

      expect(notification.channel).to.equal('recluse_event')
    })
  })
}))
