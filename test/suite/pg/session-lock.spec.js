const {createContext} = require('../../../src/async.js')
const {acquireSessionLock} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('Postgres session locking', () => {
  let context, logger
  const pgHelper = createTestHelper(async () => {
    await context.cancel()
  })

  beforeEach(async () => {
    logger = createLogger()
    context = await createContext(logger)
  })

  it('should only allow one lock to be acquired at a time', async () => {
    /* eslint-disable jest/valid-expect-in-promise */
    const log = []
    const contextA = await createContext({context})
    const contextB = await createContext({context})
    const acquireA = acquireSessionLock(contextA, logger, pgHelper.client, 111)
    const acquireB = acquireA.then(() => acquireSessionLock(contextB, logger, pgHelper.client, 111))

    await Promise.all([
      acquireB.then(() => log.push('b')).then(() => contextB.cancel()),
      acquireA.then(() => log.push('a')).then(() => contextA.cancel()),
    ])

    expect(log).toEqual(['a', 'b'])
    /* eslint-enable jest/valid-expect-in-promise */
  })
})
