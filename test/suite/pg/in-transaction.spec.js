const {createContext} = require('../../../src/async.js')
const {inTransaction} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('inTransaction()', () => {
  let cancel, client, context, logger

  const pgHelper = createTestHelper({
    async beforeEach () {
      pgHelper.trackSchemas('test')
      await pgHelper.inTransaction(async client => {
        await client.query('CREATE SCHEMA test')
        await client.query('CREATE TABLE test.entries (entry TEXT)')
      })

      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      client = await pgHelper.pool.connect()
    },

    afterEach () {
      client && client.release(true)
      cancel && cancel()
    },
  })

  it('should commit the transaction on success', async () => {
    await inTransaction(context, logger, client, async () => {
      await client.query("INSERT INTO test.entries VALUES ('a')")
      await client.query("INSERT INTO test.entries VALUES ('b')")
    })
    const {rows} = await client.query('SELECT * FROM test.entries')

    expect(rows).toEqual([
      {entry: 'a'},
      {entry: 'b'},
    ])
  })

  it('should roll back the transaction on success', async () => {
    const task = inTransaction(context, logger, client, async () => {
      await client.query("INSERT INTO test.entries VALUES ('a')")

      throw new Error('error-a')
    })

    await expect(task).rejects.toThrow('error-a')

    const {rows} = await client.query('SELECT * FROM test.entries')

    expect(rows).toEqual([])
  })
})
