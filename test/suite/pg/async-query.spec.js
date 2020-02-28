const {Canceled, createContext} = require('../../../src/async.js')
const {asyncQuery} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('Postgres async queries', () => {
  let context, logger

  const pgHelper = createTestHelper({
    async beforeEach (pgHelper) {
      await pgHelper.inTransaction(async client => {
        await client.query('CREATE SCHEMA recluse')
        await client.query('CREATE TABLE recluse.log (entry text NOT NULL)')
        await client.query("INSERT INTO recluse.log VALUES ('a')")
        await client.query("INSERT INTO recluse.log VALUES ('b')")
      })
      pgHelper.trackSchemas('recluse')

      logger = createLogger()
      context = await createContext(logger)
    },

    async afterEach () {
      if (context) await context.cancel()
    },
  })

  it('should be able to query rows one at a time', async () => {
    const readRow = asyncQuery(logger, pgHelper.pool, 'SELECT * FROM recluse.log')

    await expect(readRow(context)).resolves.toEqual([false, {entry: 'a'}])
    await expect(readRow(context)).resolves.toEqual([false, {entry: 'b'}])
    await expect(readRow(context)).resolves.toEqual([true, undefined])
    await expect(readRow(context)).resolves.toEqual([true, undefined])
  })

  it('should support cancellation', async () => {
    const readRow = asyncQuery(logger, pgHelper.pool, 'SELECT * FROM recluse.log')

    await expect(readRow(context)).resolves.toEqual([false, {entry: 'a'}])

    await context.cancel()

    await expect(readRow(context)).rejects.toThrow(Canceled)
  })

  it('should handle query errors', async () => {
    const readRow = asyncQuery(logger, pgHelper.pool, 'SELECT * FROM recluse.nonexistent')

    await expect(readRow(context)).rejects.toThrow('relation "recluse.nonexistent" does not exist')
  })
})
