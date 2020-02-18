const {appendEvents} = require('../../src/event.js')
const {maintainProjection} = require('../../src/projection.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')
const {consumeAsyncIterable} = require('../helper/async.js')
const {createClock} = require('../helper/clock.js')
const {createLogger} = require('../helper/logging.js')
const {createTestHelper} = require('../helper/pg.js')

describe('maintainProjection()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    pgHelper.trackSchemas('recluse')
    await initializeSchema(pgHelper.client)
  })

  const logger = createLogger()
  const projectionQuery = 'SELECT * FROM recluse.test_projection'
  const nameA = 'projection-name-a'
  const streamTypeA = 'stream-type-a'
  const streamInstanceA = 'stream-instance-a'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: 'a'}
  const eventB = {type: eventTypeB, data: 'b'}
  const eventC = {type: eventTypeA, data: 'c'}
  const eventD = {type: eventTypeB, data: 'd'}

  let projection

  beforeEach(async () => {
    await pgHelper.query(`
      CREATE TABLE IF NOT EXISTS recluse.test_projection
      (
        id int NOT NULL,
        value text NOT NULL,

        PRIMARY KEY (id)
      )
    `)

    let applyCount = 0

    projection = {
      async applyEvent (pgClient, event) {
        const id = applyCount++
        const value = event.data.toString()

        await pgClient.query('INSERT INTO recluse.test_projection (id, value) VALUES ($1, $2)', [id, value])

        return id
      },
    }
  })

  describe('before iteration', () => {
    it('should support cancellation', async () => {
      await maintainProjection(logger, serialization, pgHelper.client, nameA, projection).cancel()

      expect(await pgHelper.query(projectionQuery)).toHaveProperty('rowCount', 0)
    })
  })

  describe('while iterating', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 0, [eventA, eventB])
    })

    it('should apply the events in the correct order', async () => {
      await consumeAsyncIterable(
        maintainProjection(logger, serialization, pgHelper.pool, nameA, projection),
        2,
        projection => projection.cancel(),
        (value, i) => expect(value).toBe(i),
      )

      expect(await pgHelper.query(projectionQuery)).toHaveProperty('rows', expect.arrayContaining([
        expect.objectContaining({id: 0, value: 'a'}),
        expect.objectContaining({id: 1, value: 'b'}),
      ]))
    })

    it('should handle errors while applying events', async () => {
      const error = new Error('You done goofed')
      const applyEvent = async () => { throw error }
      const projection = maintainProjection(logger, serialization, pgHelper.pool, nameA, {applyEvent})

      await expect(consumeAsyncIterable(projection, 1)).rejects.toThrow(error)

      await projection.cancel()
    })

    it('should be able to apply new events when relying solely on notifications', async () => {
      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(logger, serialization, pgHelper.pool, nameA, projection, {timeout: null}),
          4,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await pgHelper.query(projectionQuery)).toHaveProperty('rows', expect.arrayContaining([
        expect.objectContaining({id: 0, value: 'a'}),
        expect.objectContaining({id: 1, value: 'b'}),
        expect.objectContaining({id: 2, value: 'c'}),
        expect.objectContaining({id: 3, value: 'd'}),
      ]))
    })

    it('should be able to apply new events when a notification is received before the timeout', async () => {
      const clock = createClock()

      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(logger, serialization, pgHelper.pool, nameA, projection, {clock}),
          4,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await pgHelper.query(projectionQuery)).toHaveProperty('rows', expect.arrayContaining([
        expect.objectContaining({id: 0, value: 'a'}),
        expect.objectContaining({id: 1, value: 'b'}),
        expect.objectContaining({id: 2, value: 'c'}),
        expect.objectContaining({id: 3, value: 'd'}),
      ]))
    })

    it('should be able to apply new events when the timeout fires before receiving a notification', async () => {
      const clock = createClock({immediate: true})

      const appendClient = await pgHelper.createClient()
      const appendClientQuery = appendClient.query.bind(appendClient)
      jest.spyOn(appendClient, 'query').mockImplementation((text, ...args) => {
        if (typeof text === 'string' && text.startsWith('NOTIFY')) return Promise.resolve()

        return appendClientQuery(text, ...args)
      })

      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(logger, serialization, pgHelper.pool, nameA, projection, {clock}),
          4,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, appendClient, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await pgHelper.query(projectionQuery)).toHaveProperty('rows', expect.arrayContaining([
        expect.objectContaining({id: 0, value: 'a'}),
        expect.objectContaining({id: 1, value: 'b'}),
        expect.objectContaining({id: 2, value: 'c'}),
        expect.objectContaining({id: 3, value: 'd'}),
      ]))
    })
  })

  describe('when resuming the maintenance of an existing projection', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 0, [eventA, eventB])
      await consumeAsyncIterable(
        maintainProjection(logger, serialization, pgHelper.pool, nameA, projection),
        2,
        projection => projection.cancel(),
      )
    })

    it('should apply new events in the correct order', async () => {
      await Promise.all([
        consumeAsyncIterable(
          maintainProjection(logger, serialization, pgHelper.pool, nameA, projection, {timeout: null}),
          2,
          projection => projection.cancel(),
        ),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])

      expect(await pgHelper.query(projectionQuery)).toHaveProperty('rows', expect.arrayContaining([
        expect.objectContaining({id: 0, value: 'a'}),
        expect.objectContaining({id: 1, value: 'b'}),
        expect.objectContaining({id: 2, value: 'c'}),
        expect.objectContaining({id: 3, value: 'd'}),
      ]))
    })
  })

  describe('when multiple workers try to maintain the same projection', () => {
    it('should cooperatively apply events using a single worker at a time', async () => {
      const maintain = () => consumeAsyncIterable(
        maintainProjection(logger, serialization, pgHelper.pool, nameA, projection, {timeout: null}),
        2,
        projection => projection.cancel(),
      )

      await Promise.all([
        maintain(),
        maintain(),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 0, [eventA, eventB, eventC, eventD]),
      ])

      expect(await pgHelper.query(projectionQuery)).toHaveProperty('rows', expect.arrayContaining([
        expect.objectContaining({id: 0, value: 'a'}),
        expect.objectContaining({id: 1, value: 'b'}),
        expect.objectContaining({id: 2, value: 'c'}),
        expect.objectContaining({id: 3, value: 'd'}),
      ]))
    })
  })
})
