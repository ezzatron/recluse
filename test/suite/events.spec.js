const {expect} = require('chai')
const {pgSpec, resolveOnCallback} = require('../helper.js')

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
        expect(await this.query(selectEvent, [0, 0])).to.have.row({...eventA, global_offset: '0'})
        expect(await this.query(selectEvent, [1, 0])).to.have.row({...eventB, global_offset: '1'})
        expect(await this.query(selectEvent, [0, 1])).to.have.row({...eventC, global_offset: '2'})
        expect(await this.query(selectEvent, [1, 1])).to.have.row({...eventD, global_offset: '3'})
      })
    })

    context('with multiple clients', function () {
      beforeEach(async function () {
        this.secondaryPgClient = this.createPgClient()
        await this.secondaryPgClient.connect()
      })

      it('should not allow concurrent writes', async function () {
        await this.pgClient.query('BEGIN')
        await this.secondaryPgClient.query('BEGIN')

        const [appendAStarted, resolveAppendAStarted] = resolveOnCallback()
        const [appendBStarted, resolveAppendBStarted] = resolveOnCallback()

        const taskA = (async () => {
          appendA1 = appendEvents(this.pgClient, typeA, nameA, 0, [eventA])
          resolveAppendAStarted()
          resultA1 = await appendA1
          await appendBStarted
          resultA2 = await appendEvents(this.pgClient, typeA, nameA, 1, [eventB])

          if (resultA1 && resultA2) {
            await this.pgClient.query('COMMIT')

            return true
          }

          try {
            await this.pgClient.query('ROLLBACK')
          } catch (e) {}

          return false
        })()

        const taskB = (async () => {
          await appendAStarted
          const appendB1 = appendEvents(this.secondaryPgClient, typeA, nameA, 0, [eventC])
          resolveAppendBStarted()
          resultB1 = await appendB1

          if (resultB1) {
            await this.secondaryPgClient.query('COMMIT')

            return true
          }

          try {
            await this.secondaryPgClient.query('ROLLBACK')
          } catch (e) {}

          return false
        })()

        const [resultA, resultB] = await Promise.all([taskA, taskB])

        expect(resultA).to.be.true()
        expect(resultB).to.be.false()
        expect(await this.query(selectEvent, [0, 0])).to.have.row(eventA)
        expect(await this.query(selectEvent, [0, 1])).to.have.row(eventB)
        expect(await this.query(selectEvent, [0, 2])).to.have.rowCount(0)
      })
    })
  })
}))
