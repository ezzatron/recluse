const {createApp} = require('../../src/app.js')
const createBankSpec = require('../fixture/app/bank/index.js')
const {createLogger} = require('../helper/logging.js')
const {createTestHelper} = require('../helper/pg.js')
const {createMockExit} = require('../helper/process.js')

describe('createApp()', () => {
  let app, exit, stop

  const pgHelper = createTestHelper(async () => {
    if (stop) await stop()
  })

  beforeEach(async () => {
    pgHelper.trackSchemas('recluse')
    const schemaName = pgHelper.trackTempSchema('recluse_test_app_bank')

    const logger = createLogger()
    exit = createMockExit(logger)

    app = createApp(createBankSpec(schemaName))
    stop = await app.run({
      // createLogger: () => logger,
      exit,
      pgConfig: pgHelper.config,
      // pgPool: pgHelper.pool,
    })
  })

  it('should be able to do this', async () => {
    // await new Promise(resolve => {
    //   setInterval(async () => {
    //     const result = await pgHelper.query('select table_name from information_schema.tables WHERE table_schema = \'recluse\'')
    //     console.log('ROWS', result.rows)
    //     if (result.rowCount > 0) resolve()
    //   }, 100)
    // })

    expect(true).toBe(true)
  })

  it('should also be able to do this', async () => {
  //   // await new Promise(resolve => {
  //   //   setInterval(async () => {
  //   //     const result = await pgHelper.query('select table_name from information_schema.tables WHERE table_schema = \'recluse\'')
  //   //     console.log('ROWS', result.rows)
  //   //     if (result.rowCount > 0) resolve()
  //   //   }, 100)
  //   // })

    expect(true).toBe(true)
  })
})
