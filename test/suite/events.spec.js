const {expect} = require('chai')
const {pgSpec} = require('../helper.js')

const {initializeSchema, appendEvents} = require('../../src/index.js')

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
    const type = 'stream-type-a'
    const name = 'stream-name-a'
    const eventA = {type: 'event-type-a', data: Buffer.from('a')}
    const eventB = {type: 'event-type-b', data: Buffer.from('b')}
    const eventC = {type: 'event-type-a', data: Buffer.from('c')}
    const eventD = {type: 'event-type-b', data: Buffer.from('d')}

    const selectEvent =
      'SELECT * FROM recluse.event WHERE stream_id = $1 AND stream_offset = $2'

    beforeEach(async function () {
      await initializeSchema(this.pgClient)
    })

    context('with a new stream', function () {
      it('should be able to append to the stream', async function () {
        expect(await appendEvents(this.pgClient, type, name, 0, [eventA, eventB])).to.be.true()
        expect(await this.query(selectEvent, [1, 0])).to.have.row(eventA)
        expect(await this.query(selectEvent, [1, 1])).to.have.row(eventB)
      })
    })
  })
}))
