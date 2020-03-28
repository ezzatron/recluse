const {createContext} = require('../../../src/async.js')
const {consumeContinuousQuery} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('consumeContinuousQuery()', () => {
  let cancel, context, logger

  const pgHelper = createTestHelper({
    async beforeEach () {
      pgHelper.trackSchemas('test')
      await pgHelper.inTransaction(async client => {
        await client.query('CREATE SCHEMA test')
        await client.query('CREATE TABLE test.entries (entry INT)')
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

  it('should be able to continuously consume a query using notifications', async () => {
    const rows = []

    let resolveFirstRowRead
    const firstRowRead = new Promise(resolve => { resolveFirstRowRead = resolve })

    const performQuery = consumeContinuousQuery(
      context,
      logger,
      pgHelper.pool,
      'test_channel',
      row => row.entry + 1,
      'SELECT * FROM test.entries WHERE entry = $1',
      {},
      async row => {
        rows.push(row)

        if (row.entry > 1) return false

        resolveFirstRowRead()

        return true
      },
    )
    const insert0And1 = performInsert(0, 1)
    const insert2And3 = firstRowRead.then(() => performInsert(2, 3))

    await Promise.all([performQuery, insert0And1, insert2And3])

    expect(rows).toEqual([
      {entry: 0},
      {entry: 1},
      {entry: 2},
    ])

    async function performInsert (...entries) {
      return pgHelper.inTransaction(async client => {
        for (const entry of entries) await client.query('INSERT INTO test.entries VALUES ($1)', [entry])
        await client.query('NOTIFY test_channel')
      })
    }
  })

  it('should be able to continuously consume a query using timeouts', async () => {
    const rows = []

    let resolveFirstRowRead
    const firstRowRead = new Promise(resolve => { resolveFirstRowRead = resolve })

    const performQuery = consumeContinuousQuery(
      context,
      logger,
      pgHelper.pool,
      'test_channel',
      row => row.entry + 1,
      'SELECT * FROM test.entries WHERE entry = $1',
      {timeout: 1},
      async row => {
        rows.push(row)

        if (row.entry > 1) return false

        resolveFirstRowRead()

        return true
      },
    )
    const insert0And1 = performInsert(0, 1)
    const insert2And3 = firstRowRead.then(() => performInsert(2, 3))

    await Promise.all([performQuery, insert0And1, insert2And3])

    expect(rows).toEqual([
      {entry: 0},
      {entry: 1},
      {entry: 2},
    ])

    async function performInsert (...entries) {
      return pgHelper.inTransaction(async client => {
        for (const entry of entries) await client.query('INSERT INTO test.entries VALUES ($1)', [entry])
      })
    }
  })

  it('should throw errors for invalid queries', async () => {
    const task = consumeContinuousQuery(
      context,
      logger,
      pgHelper.pool,
      'test_channel',
      () => 0,
      'SELECT * FROM nonexistent',
      {},
      () => false,
    )

    await expect(task).rejects.toThrow('relation "nonexistent" does not exist')
  })
})
