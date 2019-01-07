const {expect} = require('chai')
const {pgSpec} = require('../helper.js')

const {appendEvents, initializeSchema} = require('../../src/index.js')

describe('Events', pgSpec(function () {
  describe('initializeSchema()', function () {
    const selectTable = "SELECT * FROM information_schema.tables WHERE table_schema = 'recluse' AND table_name = $1"

    it('should create the necessary tables', async function () {
      await initializeSchema(this.pgClient)

      expect(await this.query(selectTable, ['global_offset'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['stream'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['event'])).to.have.rowCount(1)
    })

    it('should be safe to run multiple times', async function () {
      await initializeSchema(this.pgClient)
      await initializeSchema(this.pgClient)

      expect(await this.query(selectTable, ['global_offset'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['stream'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['event'])).to.have.rowCount(1)
    })
  })

  describe('appendEvents()', function () {
    const typeA = 'stream-type-a'
    const nameA = 'stream-name-a'
    const nameB = 'stream-name-b'
    const eventTypeA = 'event-type-a'
    const eventTypeB = 'event-type-b'
    const eventA = {type: eventTypeA, data: Buffer.from('a')}
    const eventB = {type: eventTypeB, data: Buffer.from('b')}
    const eventC = {type: eventTypeA, data: Buffer.from('c')}
    const eventD = {type: eventTypeB, data: Buffer.from('d')}

    const selectEvent = 'SELECT * FROM recluse.event WHERE stream_id = $1 AND stream_offset = $2'

    beforeEach(async function () {
      await initializeSchema(this.pgClient)
    })

    context('with a new stream', function () {
      it('should be able to append to the stream', async function () {
        const actual =
          await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB]))

        expect(actual).to.be.true()
        expect(await this.query(selectEvent, [0, 0])).to.have.row(eventA)
        expect(await this.query(selectEvent, [0, 1])).to.have.row(eventB)
      })
    })

    context('with an existing stream', function () {
      beforeEach(async function () {
        await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 0, [eventA, eventB]))
      })

      it('should be able to append to the stream', async function () {
        const actual = await this.inTransaction(
          async () => appendEvents(this.pgClient, typeA, nameA, 2, [eventC, eventD])
        )

        expect(actual).to.be.true()
        expect(await this.query(selectEvent, [0, 2])).to.have.row(eventC)
        expect(await this.query(selectEvent, [0, 3])).to.have.row(eventD)
      })

      it('should fail if the specified offset is less than the next stream offset', async function () {
        const actual =
          await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 1, [eventC, eventD]))

        expect(actual).to.be.false()
        expect(await this.query(selectEvent, [0, 2])).to.have.rowCount(0)
        expect(await this.query(selectEvent, [0, 3])).to.have.rowCount(0)
      })

      it('should fail if the specified offset is greater than the next stream offset', async function () {
        const actual =
          await this.inTransaction(async () => appendEvents(this.pgClient, typeA, nameA, 3, [eventC, eventD]))

        expect(actual).to.be.false()
        expect(await this.query(selectEvent, [0, 2])).to.have.rowCount(0)
        expect(await this.query(selectEvent, [0, 3])).to.have.rowCount(0)
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
        expect(await appendEvents(this.pgClient, typeA, nameA, 3, [eventC, eventD])).to.be.false()
        expect(await this.query(selectEvent, [0, 0])).to.have.row({...eventA, global_offset: '0'})
        expect(await this.query(selectEvent, [1, 0])).to.have.row({...eventB, global_offset: '1'})
        expect(await this.query(selectEvent, [0, 1])).to.have.row({...eventC, global_offset: '2'})
        expect(await this.query(selectEvent, [1, 1])).to.have.row({...eventD, global_offset: '3'})
      })
    })
  })
}))
