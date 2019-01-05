const {expect} = require('chai')
const {pgSpec} = require('../helper.js')

const {initializeSchema, appendEvents} = require('../../src/index.js')

describe('Events', pgSpec(function () {
  describe('initializeSchema()', function () {
    it('should create the necessary tables', async function () {
      await initializeSchema(this.pgClient)
      const actual = await this.pgClient.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'recluse'"
      )

      expect(actual.rows).to.deep.include({table_name: 'global_offset'})
      expect(actual.rows).to.deep.include({table_name: 'stream'})
      expect(actual.rows).to.deep.include({table_name: 'event'})
    })

    it('should be safe to run multiple times', async function () {
      await initializeSchema(this.pgClient)
      await initializeSchema(this.pgClient)
      const actual = await this.pgClient.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'recluse'"
      )

      expect(actual.rows).to.deep.include({table_name: 'global_offset'})
      expect(actual.rows).to.deep.include({table_name: 'stream'})
      expect(actual.rows).to.deep.include({table_name: 'event'})
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
        const events = [
          {type: 'event-type-a', data: Buffer.from('a')},
          {type: 'event-type-b', data: Buffer.from('b')},
        ]

        await appendEvents(this.pgClient, type, name, start, events)
        const actualStream = await this.pgClient.query('SELECT * FROM recluse.stream WHERE name = $1', [name])

        expect(actualStream.rowCount).to.equal(1)
        expect(actualStream.rows[0]).to.include({type, name})

        const {id: streamId} = actualStream.rows[0]
        const actualEvents = await this.pgClient.query(
          'SELECT * FROM recluse.event WHERE stream_id = $1 ORDER BY stream_offset',
          [streamId]
        )

        expect(actualEvents.rowCount).to.equal(2)
        expect(actualEvents.rows[0].type).to.equal(events[0].type)
        expect(actualEvents.rows[0].data).to.equalBytes(events[0].data)
        expect(actualEvents.rows[1].type).to.equal(events[1].type)
        expect(actualEvents.rows[1].data).to.equalBytes(events[1].data)
      })
    })
  })
}))
