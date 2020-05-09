const {createContext} = require('../../../src/async.js')
const {withAdvisoryLock} = require('../../../src/pg.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('withAdvisoryLock()', () => {
  let cancel, client, context, log, logger, runPromises

  const pgHelper = createTestHelper({
    async beforeEach () {
      runPromises = () => new Promise(resolve => { setImmediate(resolve) })

      log = []
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      client = await pgHelper.pool.connect()
    },

    async afterEach () {
      client && client.release(true)
      await cancel()
    },
  })

  it('should only allow one lock to be acquired at a time', async () => {
    function writeToLog () {
      return withAdvisoryLock(context, logger, client, 111, 222, async () => {
        log.push('a')
        await runPromises()
        log.push('b')
      })
    }

    await Promise.all([writeToLog(), writeToLog(), writeToLog(), writeToLog()])

    expect(log).toEqual(['a', 'b', 'a', 'b', 'a', 'b', 'a', 'b'])
  })

  it('should handle termination during lock acquisition', async () => {
    let resolveUnlocked
    const unlocked = new Promise(resolve => { resolveUnlocked = resolve })

    const client = {
      async query (text) {
        cancel()

        if (text.includes('pg_advisory_unlock')) resolveUnlocked()
      },
      release () {},
    }

    await expect(withAdvisoryLock(context, logger, client, 111, 222, async () => {})).rejects.toThrow('Canceled')
    await expect(unlocked).resolves.toBeUndefined()
  })

  it('should handle termination during lock acquisition, and subsequent lock acquisition errors', async () => {
    let resolveThrown
    const thrown = new Promise(resolve => { resolveThrown = resolve })

    const client = {
      async query (text) {
        cancel()

        if (text.includes('pg_advisory_lock')) {
          setImmediate(resolveThrown)

          throw new Error('error-a')
        }
      },
      release () {},
    }

    jest.spyOn(client, 'query')
    jest.spyOn(logger, 'debug')

    await expect(withAdvisoryLock(context, logger, client, 111, 222, async () => {})).rejects.toThrow('Canceled')
    await expect(thrown).resolves.toBeUndefined()
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('Postgres advisory lock acquisition failed during cleanup: Error: error-a'),
    )
    expect(client.query).not.toHaveBeenCalledWith(expect.stringContaining('pg_advisory_unlock'), expect.anything())
  })

  it('should handle termination during lock acquisition, and subsequent lock release errors', async () => {
    let resolveUnlocked
    const unlocked = new Promise(resolve => { resolveUnlocked = resolve })

    const client = {
      async query (text) {
        cancel()

        if (text.includes('pg_advisory_unlock')) {
          setImmediate(resolveUnlocked)

          throw new Error('error-a')
        }
      },
      release () {},
    }

    jest.spyOn(logger, 'warn')

    await expect(withAdvisoryLock(context, logger, client, 111, 222, async () => {})).rejects.toThrow('Canceled')
    await expect(unlocked).resolves.toBeUndefined()
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('Unable to cleanly release Postgres advisory lock: Error: error-a'),
    )
  })
})
