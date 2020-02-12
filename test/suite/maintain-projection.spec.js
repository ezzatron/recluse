const {createSandbox, match} = require('sinon')
const {expect} = require('chai')

const {consumeAsyncIterable, pgSpec} = require('../helper.js')
const {createClock} = require('../clock.js')

const {appendEvents} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')
const {maintainProjection} = require('../../src/projection.js')
const {serialization} = require('../../src/serialization/json.js')

describe('maintainProjection()', pgSpec(function () {
  const nameA = 'projection-name-a'
  const streamTypeA = 'stream-type-a'
  const streamInstanceA = 'stream-instance-a'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: 'a'}
  const eventB = {type: eventTypeB, data: 'b'}
  const eventC = {type: eventTypeA, data: 'c'}
  const eventD = {type: eventTypeB, data: 'd'}

  beforeEach(async function () {
    this.sandbox = createSandbox()

    await initializeSchema(this.pgClient)

    await this.query(`
      CREATE TABLE IF NOT EXISTS recluse.test_projection
      (
        id int NOT NULL,
        value text NOT NULL,

        PRIMARY KEY (id)
      )
    `)

    let applyCount = 0

    this.projection = {
      async applyEvent (pgClient, event) {
        const id = applyCount++
        const value = event.data.toString()

        await pgClient.query('INSERT INTO recluse.test_projection (id, value) VALUES ($1, $2)', [id, value])

        return id
      },
    }

    this.projectionQuery = 'SELECT * FROM recluse.test_projection'
  })

  afterEach(function () {
    this.sandbox.restore()
  })

  context('before iteration', function () {
    it('should support cancellation', async function () {
      await maintainProjection(serialization, this.pgClient, nameA, this.projection).cancel()

      expect(await this.query(this.projectionQuery)).to.have.rowCount(0)
    })
  })

  context('while iterating', function () {
    beforeEach(async function () {
      await appendEvents(serialization, this.pgClient, streamTypeA, streamInstanceA, 0, [eventA, eventB])
    })

    it('should apply the events in the correct order', async function () {
      await consumeAsyncIterable(
        maintainProjection(serialization, this.pgPool, nameA, this.projection),
        2,
        projection => projection.cancel(),
        (value, i) => expect(value).to.equal(i),
      )

      expect(await this.query(this.projectionQuery)).to.have.rows([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
      ])
    })

    it('should handle errors while applying events', async function () {
      const releases = []
      const pool = {
        connect: async () => {
          const client = await this.createPgClient()
          this.sandbox.spy(client, 'release')
          releases.push(client.release)

          return client
        },
      }
      const error = new Error('You done goofed')
      const applyEvent = async () => { throw error }
      const projection = maintainProjection(serialization, pool, nameA, {applyEvent})

      await expect(consumeAsyncIterable(projection, 1)).to.be.rejectedWith(error)

      await projection.cancel()
    })

    it('should be able to apply new events when relying solely on notifications', async function () {
      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(serialization, this.pgPool, nameA, this.projection, {timeout: null}),
          4,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, this.pgClient, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await this.query(this.projectionQuery)).to.have.rows([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })

    it('should be able to apply new events when a notification is received before the timeout', async function () {
      const clock = createClock()

      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(serialization, this.pgPool, nameA, this.projection, {clock}),
          4,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, this.pgClient, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await this.query(this.projectionQuery)).to.have.rows([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })

    it('should be able to apply new events when the timeout fires before receiving a notification', async function () {
      const clock = createClock({immediate: true})

      const appendClient = await this.createPgClient()
      this.sandbox.stub(appendClient, 'query')
      appendClient.query.withArgs(match('NOTIFY')).callsFake(async () => {})
      appendClient.query.callThrough()

      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(serialization, this.pgPool, nameA, this.projection, {clock}),
          4,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, appendClient, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await this.query(this.projectionQuery)).to.have.rows([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })
  })

  context('when resuming the maintenance of an existing projection', function () {
    beforeEach(async function () {
      await appendEvents(serialization, this.pgClient, streamTypeA, streamInstanceA, 0, [eventA, eventB])
      await consumeAsyncIterable(
        maintainProjection(serialization, this.pgPool, nameA, this.projection),
        2,
        projection => projection.cancel(),
      )
    })

    it('should apply new events in the correct order', async function () {
      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(serialization, this.pgPool, nameA, this.projection, {timeout: null}),
          2,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, this.pgClient, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await this.query(this.projectionQuery)).to.have.rows([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })
  })

  context('when multiple workers try to maintain the same projection', function () {
    it('should cooperatively apply events using a single worker at a time', async function () {
      const maintain = () => consumeAsyncIterable(
        maintainProjection(serialization, this.pgPool, nameA, this.projection, {timeout: null}),
        2,
        projection => projection.cancel(),
      )

      await Promise.all([
        maintain(),
        maintain(),

        appendEvents(serialization, this.pgClient, streamTypeA, streamInstanceA, 0, [eventA, eventB, eventC, eventD]),
      ])

      expect(await this.query(this.projectionQuery)).to.have.rows([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })
  })
}))
