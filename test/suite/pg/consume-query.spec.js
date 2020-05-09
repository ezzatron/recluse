const {createContext} = require('../../../src/async.js')
const {consumeQuery} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('consumeQuery()', () => {
  let cancel, client, context, logger

  const pgHelper = createTestHelper({
    async beforeEach () {
      pgHelper.trackSchemas('test')
      await pgHelper.inTransaction(async client => {
        await client.query('CREATE SCHEMA test')
        await client.query('CREATE TABLE test.entries (entry INT)')
        await client.query('INSERT INTO test.entries VALUES (0)')
        await client.query('INSERT INTO test.entries VALUES (1)')
        await client.query('INSERT INTO test.entries VALUES (2)')
      })

      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      client = await pgHelper.pool.connect()
    },

    async afterEach () {
      client && client.release(true)
      await cancel()
    },
  })

  it('should be able to completely consume a query', async () => {
    const rows = []

    const wasConsumed = await consumeQuery(context, logger, client, 'SELECT * FROM test.entries', {}, async row => {
      rows.push(row)

      return true
    })

    expect(wasConsumed).toBe(true)
    expect(rows).toEqual([
      {entry: 0},
      {entry: 1},
      {entry: 2},
    ])
  })

  it('should be able to partially consume a query', async () => {
    const rows = []

    const wasConsumed = await consumeQuery(context, logger, client, 'SELECT * FROM test.entries', {}, async row => {
      rows.push(row)

      return row.entry < 1
    })

    expect(wasConsumed).toBe(false)
    expect(rows).toEqual([
      {entry: 0},
      {entry: 1},
    ])
  })

  it('should throw errors for invalid queries', async () => {
    const task = consumeQuery(context, logger, client, 'SELECT * FROM nonexistent', {}, () => false)

    await expect(task).rejects.toThrow('relation "nonexistent" does not exist')
  })
})
