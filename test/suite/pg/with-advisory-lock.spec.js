const {createContext} = require('../../../src/async.js')
const {withAdvisoryLock} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('withAdvisoryLock()', () => {
  let cancel, context, log, logger, runAllJestTimers, runPromises, sleep

  const pgHelper = createTestHelper({
    async beforeEach () {
      jest.useFakeTimers()

      sleep = delay => new Promise(resolve => { setTimeout(resolve, delay) })
      runPromises = () => new Promise(resolve => { setImmediate(resolve) })
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
    const namespace = 111
    const id = 222

    const taskA = sleep(111).then(() => withAdvisoryLock(context, logger, pgHelper.pool, namespace, id, async () => {
      await sleep(222)
      log.push('a')
    }))
    const taskB = sleep(222).then(() => withAdvisoryLock(context, logger, pgHelper.pool, namespace, id, async () => {
      log.push('b')
    }))

    jest.runAllTimers()
    await Promise.all([taskA, taskB])

    expect(log).toEqual(['a', 'b'])
  })
})
