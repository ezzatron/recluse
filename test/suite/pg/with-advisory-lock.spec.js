const {createContext} = require('../../../src/async.js')
const {withAdvisoryLock} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('withAdvisoryLock()', () => {
  let cancel, context, log, logger, runAllJestTimers, sleep

  const pgHelper = createTestHelper({
    async beforeEach () {
      jest.useFakeTimers()

      sleep = delay => new Promise(resolve => { setTimeout(resolve, delay) })
      runAllJestTimers = async () => { jest.runAllTimers() }

      log = []
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]
    },

    async afterEach () {
      await cancel()
    },
  })

  it('should only allow one lock to be acquired at a time', async () => {
    /* eslint-disable jest/valid-expect-in-promise */

    const namespace = 111
    const id = 222

    await Promise.all([
      withAdvisoryLock(context, logger, pgHelper.pool, namespace, id, async () => {
        await sleep(111)
        log.push('a')
      }),
      withAdvisoryLock(context, logger, pgHelper.pool, namespace, id, async () => {
        log.push('b')
      }),
      runAllJestTimers,
    ])

    expect(log).toEqual(['a', 'b'])
    /* eslint-enable jest/valid-expect-in-promise */
  })
})
