const {createContext} = require('../../../src/async.js')
const {consumeQuery} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('consumeQuery()', () => {
  let cancel, context, logger

  const pgHelper = createTestHelper({
    async beforeEach () {
      pgHelper.trackSchemas('test')
      await pgHelper.inTransaction(async client => {
        await client.query('CREATE SCHEMA test')
        await client.query('CREATE TABLE test.entries (entry TEXT)')
        await client.query("INSERT INTO test.entries VALUES ('a')")
        await client.query("INSERT INTO test.entries VALUES ('b')")
        await client.query("INSERT INTO test.entries VALUES ('c')")
      })

      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]
    },

    async afterEach () {
      await cancel()
    },
  })

  it('should be able to completely consume a query', async () => {
    const rows = []

    await consumeQuery(context, logger, pgHelper.pool, 'SELECT * FROM test.entries', [], async row => {
      rows.push(row)

      return true
    })

    expect(rows).toEqual([
      {entry: 'a'},
      {entry: 'b'},
      {entry: 'c'},
    ])
  })

  it('should be able to partially consume a query', async () => {
    const rows = []

    await consumeQuery(context, logger, pgHelper.pool, 'SELECT * FROM test.entries', [], async row => {
      rows.push(row)

      return row.entry !== 'b'
    })

    expect(rows).toEqual([
      {entry: 'a'},
      {entry: 'b'},
    ])
  })

  it('should throw errors for invalid queries', async () => {
    const task = consumeQuery(context, logger, pgHelper.pool, 'SELECT * FROM nonexistent')

    await expect(task).rejects.toThrow('relation "nonexistent" does not exist')
  })
})
