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

  it('should handle termination during client acquisition, and subsequent pool connect errors', async () => {
    let resolveThrown
    const thrown = new Promise(resolve => { resolveThrown = resolve })

    const client = {
      release () {},
    }
    const pool = {
      async connect () {
        cancel()

        setImmediate(() => resolveThrown())
        throw new Error('error-a')
      },
    }

    jest.spyOn(client, 'release')
    jest.spyOn(logger, 'debug')

    await expect(withAdvisoryLock(context, logger, pool, 111, 222, async () => {})).rejects.toThrow('Canceled')
    await expect(thrown).resolves.toBeUndefined()
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('Postgres client acquisition failed during cleanup: Error: error-a'),
    )
    expect(client.release).not.toHaveBeenCalled()
  })

  it('should handle termination during client acquisition, and subsequent client release errors', async () => {
    let resolveReleased
    const released = new Promise(resolve => { resolveReleased = resolve })

    const client = {
      release () {
        setImmediate(() => resolveReleased())

        throw new Error('error-a')
      },
    }
    const pool = {
      async connect () {
        cancel()

        return client
      },
    }

    jest.spyOn(logger, 'warn')

    await expect(withAdvisoryLock(context, logger, pool, 111, 222, async () => {})).rejects.toThrow('Canceled')
    await expect(released).resolves.toBeUndefined()
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('Unable to cleanly release Postgres client: Error: error-a'),
    )
  })
})
