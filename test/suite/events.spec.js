const {expect} = require('chai')
const {pgSpec} = require('../helper.js')

const {initializeSchema, appendEvents} = require('../../src/index.js')

describe('Events', pgSpec(function () {
  describe('initializeSchema()', function () {
    it('should create the necessary tables', async function () {
      await initializeSchema(this.pgClient)

      const selectTable = "SELECT * FROM information_schema.tables WHERE table_schema = 'recluse' AND table_name = $1"
      expect(await this.query(selectTable, ['global_offset'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['stream'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['event'])).to.have.rowCount(1)
    })

    it('should be safe to run multiple times', async function () {
      await initializeSchema(this.pgClient)
      await initializeSchema(this.pgClient)

      const selectTable = "SELECT * FROM information_schema.tables WHERE table_schema = 'recluse' AND table_name = $1"
      expect(await this.query(selectTable, ['global_offset'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['stream'])).to.have.rowCount(1)
      expect(await this.query(selectTable, ['event'])).to.have.rowCount(1)
    })
  })

  context('with initialized schema', function () {
    beforeEach(async function () {
      await initializeSchema(this.pgClient)
    })

    describe('appendEvents()', function () {
      it('should be able to append to the beginning of a new stream', async function () {
        const type = 'stream-type-a'
        const name = 'stream-name-a'
        const start = 0
        const eventA = {type: 'event-type-a', data: Buffer.from('a')}
        const eventB = {type: 'event-type-b', data: Buffer.from('b')}
        const events = [eventA, eventB]
        await appendEvents(this.pgClient, type, name, start, events)

        const selectEvent =
          'SELECT * FROM recluse.event WHERE stream_id = $1 AND stream_offset = $2 AND type = $3 AND data = $4'
        expect(await this.query(selectEvent, [1, 0, eventA.type, eventA.data])).to.have.rowCount(1)
        expect(await this.query(selectEvent, [1, 1, eventB.type, eventB.data])).to.have.rowCount(1)
      })
    })
  })
}))
