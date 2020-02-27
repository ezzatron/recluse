const {Canceled, createContext} = require('../../../src/async.js')
const {asyncQuery} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('Postgres async queries', () => {
  let context, logger
  const pgHelper = createTestHelper(async () => {
    await context.cancel()
  })

  beforeEach(async () => {
    pgHelper.trackSchemas('recluse')
    await pgHelper.query('CREATE SCHEMA recluse')
    await pgHelper.query('CREATE TABLE recluse.log (entry text NOT NULL)')
    await pgHelper.query("INSERT INTO recluse.log VALUES ('a')")
    await pgHelper.query("INSERT INTO recluse.log VALUES ('b')")

    logger = createLogger()
    context = await createContext(logger)
  })

  it('should be able to query rows one at a time', async () => {
    const readAsyncQueryRow = asyncQuery(logger, pgHelper.client, 'SELECT * FROM recluse.log')

    await expect(readAsyncQueryRow(context)).resolves.toEqual([false, {entry: 'a'}])
    await expect(readAsyncQueryRow(context)).resolves.toEqual([false, {entry: 'b'}])
    await expect(readAsyncQueryRow(context)).resolves.toEqual([true, undefined])
    await expect(readAsyncQueryRow(context)).resolves.toEqual([true, undefined])
  })

  it('should support cancellation', async () => {
    const readAsyncQueryRow = asyncQuery(logger, pgHelper.client, 'SELECT * FROM recluse.log')

    await expect(readAsyncQueryRow(context)).resolves.toEqual([false, {entry: 'a'}])

    await context.cancel()

    await expect(readAsyncQueryRow(context)).rejects.toThrow(Canceled)
  })

  it('should handle query errors', async () => {
    const readAsyncQueryRow = asyncQuery(logger, pgHelper.client, 'SELECT * FROM recluse.nonexistent')

    await expect(readAsyncQueryRow(context)).rejects.toThrow('relation "recluse.nonexistent" does not exist')
  })
})
