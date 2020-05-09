const {configure} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('configure()', () => {
  let logger

  const pgHelper = createTestHelper({
    async beforeEach () {
      pgHelper.trackSchemas('test')
      await pgHelper.inTransaction(async client => {
        await client.query('CREATE SCHEMA test')
        await client.query('CREATE TABLE test.entries (entry TEXT)')
      })

      logger = createLogger()
    },
  })

  it('should cause timestamp types to be returned as strings', async () => {
    const restore = configure(logger)
    const {rows: [{timestamp, timestamptz}]} =
      await pgHelper.inTransaction(client => client.query('SELECT NOW()::timestamp AS timestamp, NOW() AS timestamptz'))

    try {
      expect(timestamp).toEqual(expect.stringMatching(/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+$/))
      expect(timestamptz).toEqual(expect.stringMatching(/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+\+\d\d$/))
    } finally {
      restore()
    }
  })

  it('should correctly restore original timestamp behavior', async () => {
    const restore = configure(logger)
    restore()
    const {rows: [{timestamp, timestamptz}]} =
      await pgHelper.inTransaction(client => client.query('SELECT NOW()::timestamp AS timestamp, NOW() AS timestamptz'))

    try {
      expect(timestamp).toBeInstanceOf(Date)
      expect(timestamptz).toBeInstanceOf(Date)
    } finally {
      restore()
    }
  })
})
