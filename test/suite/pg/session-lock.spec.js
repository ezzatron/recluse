const {createContext} = require('../../../src/async.js')
const {acquireSessionLock} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('Postgres session locking', () => {
  let context, logger

  const pgHelper = createTestHelper({
    async beforeEach () {
      logger = createLogger()
      context = await createContext(logger)
    },

    async afterEach () {
      await context.cancel()
    },
  })

  it('should only allow one lock to be acquired at a time', async () => {
    /* eslint-disable jest/valid-expect-in-promise */
    const log = []

    const acquireA = acquireSessionLock(context, logger, pgHelper.pool, 111)
    const acquireB = acquireA.then(() => acquireSessionLock(context, logger, pgHelper.pool, 111))

    await Promise.all([
      acquireB.then(release => {
        log.push('b')

        return release(context)
      }),

      acquireA.then(release => {
        log.push('a')

        return release(context)
      }),
    ])

    expect(log).toEqual(['a', 'b'])
    /* eslint-enable jest/valid-expect-in-promise */
  })
})
