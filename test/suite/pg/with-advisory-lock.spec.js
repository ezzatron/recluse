const {createContext} = require('../../../src/async.js')
const {withAdvisoryLock} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('withAdvisoryLock()', () => {
  let cancel, context, log, logger, runPromises

  const pgHelper = createTestHelper({
    async beforeEach () {
      runPromises = () => new Promise(resolve => { setImmediate(resolve) })

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
    function writeToLog () {
      return withAdvisoryLock(context, logger, pgHelper.pool, 111, 222, async () => {
        log.push('a')
        await runPromises()
        log.push('b')
      })
    }

    await Promise.all([writeToLog(), writeToLog(), writeToLog(), writeToLog()])

    expect(log).toEqual(['a', 'b', 'a', 'b', 'a', 'b', 'a', 'b'])
  })

  it('should handle termination during client acquisition', async () => {
    let resolveReleased
    const released = new Promise(resolve => { resolveReleased = resolve })

    const client = {
      release () {
        resolveReleased()
      },
    }
    const pool = {
      async connect () {
        cancel()

        return client
      },
    }

    await expect(withAdvisoryLock(context, logger, pool, 111, 222, async () => {})).rejects.toThrow('Canceled')
    await expect(released).resolves.toBeUndefined()
  })
})
