const {createContext} = require('../../../src/async.js')
const {appendEvents} = require('../../../src/event.js')
const {configure, inTransaction} = require('../../../src/pg.js')
const {maintainProjection} = require('../../../src/projection.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('maintainProjection()', () => {
  const nameA = 'projection-name-a'
  const streamTypeA = 'stream-type-a'
  const streamInstanceA = 'stream-instance-a'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const eventA = {type: eventTypeA, data: 'a'}
  const eventB = {type: eventTypeB, data: 'b'}
  const eventC = {type: eventTypeA, data: 'c'}
  const eventD = {type: eventTypeB, data: 'd'}

  function afterDelay (delay, fn) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        fn().then(resolve, reject)
      }, delay)
    })
  }

  function shouldContinueTimes (total) {
    let callCount = 0

    return () => ++callCount < total
  }

  let cancel, context, logger, projection, readProjection, restore

  const pgHelper = createTestHelper({
    async beforeEach () {
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      restore = configure()
      pgHelper.trackSchemas('recluse')
      await initializeSchema(context, logger, pgHelper.pool)

      await pgHelper.inTransaction(async client => {
        await client.query(`
          CREATE TABLE IF NOT EXISTS recluse.test_projection
          (
            id int NOT NULL,
            value text NOT NULL,

            PRIMARY KEY (id)
          )
        `)
      })

      let applyCount = 0

      projection = {
        async applyEvent ({client, event}) {
          const id = applyCount++
          const value = event.data.toString()

          await client.query('INSERT INTO recluse.test_projection (id, value) VALUES ($1, $2)', [id, value])

          return id
        },
      }

      readProjection = async () => {
        return pgHelper.inTransaction(async client => {
          const result = await client.query('SELECT * FROM recluse.test_projection')

          return result.rows
        })
      }
    },

    afterEach () {
      restore && restore()
      cancel && cancel()
    },
  })

  describe('before applying events', () => {
    it('should support cancellation', async () => {
      const task = Promise.all([
        maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection),
        cancel(),
      ])

      await expect(task).rejects.toThrow('Canceled')
    })
  })

  describe('while running', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 0, [eventA, eventB])
      })
    })

    it('should apply the events in the correct order', async () => {
      await maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
        shouldContinue: shouldContinueTimes(2),
      })

      expect(await readProjection()).toEqual([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
      ])
    })

    it('should throw if events cannot be applied', async () => {
      const error = new Error('You done goofed')
      const applyEvent = async () => { throw error }
      const task = maintainProjection(context, logger, pgHelper.pool, serialization, nameA, {applyEvent})

      await expect(task).rejects.toThrow(error)
    })

    it('should apply new events when relying solely on notifications', async () => {
      await Promise.all([
        maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
          shouldContinue: shouldContinueTimes(4),
          timeout: null,
        }),
        pgHelper.inTransaction(async client => {
          await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 2, [eventC, eventD])
        }),
      ])

      expect(await readProjection()).toEqual([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })

    it('should apply new events when a notification is received before the timeout', async () => {
      await Promise.all([
        maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
          shouldContinue: shouldContinueTimes(4),
          timeout: 100,
        }),
        afterDelay(10, async () => {
          await pgHelper.inTransaction(async client => {
            const events = [eventC, eventD]
            await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 2, events)
          })
        }),
      ])

      expect(await readProjection()).toEqual([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })

    it('should apply new events when the timeout fires before receiving a notification', async () => {
      const appendClient = await pgHelper.pool.connect()
      const appendClientQuery = appendClient.query.bind(appendClient)
      jest.spyOn(appendClient, 'query').mockImplementation((text, ...args) => {
        if (typeof text === 'string' && text.startsWith('NOTIFY')) return Promise.resolve()

        return appendClientQuery(text, ...args)
      })

      try {
        await Promise.all([
          maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
            shouldContinue: shouldContinueTimes(4),
            timeout: 5,
          }),
          afterDelay(10, async () => {
            await inTransaction(context, logger, appendClient, async () => {
              const events = [eventC, eventD]
              await appendEvents(context, logger, appendClient, serialization, streamTypeA, streamInstanceA, 2, events)
            })
          }),
        ])
      } finally {
        appendClient.release()
      }

      expect(await readProjection()).toEqual([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })
  })

  describe('when resuming the maintenance of an existing projection', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 0, [eventA, eventB])
      })

      await maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
        shouldContinue: shouldContinueTimes(2),
      })
    })

    it('should apply new events in the correct order', async () => {
      await Promise.all([
        maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
          shouldContinue: shouldContinueTimes(2),
        }),
        pgHelper.inTransaction(async client => {
          await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 2, [eventC, eventD])
        }),
      ])

      expect(await readProjection()).toEqual([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })
  })

  describe('when multiple workers try to maintain the same projection', () => {
    it('should cooperatively apply events using a single worker at a time', async () => {
      await Promise.all([
        maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
          shouldContinue: shouldContinueTimes(2),
        }),
        maintainProjection(context, logger, pgHelper.pool, serialization, nameA, projection, {
          shouldContinue: shouldContinueTimes(2),
        }),
        pgHelper.inTransaction(async client => {
          const events = [eventA, eventB, eventC, eventD]
          await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 0, events)
        }),
      ])

      expect(await readProjection()).toEqual([
        {id: 0, value: 'a'},
        {id: 1, value: 'b'},
        {id: 2, value: 'c'},
        {id: 3, value: 'd'},
      ])
    })
  })
})
