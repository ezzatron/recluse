const {createContext} = require('../../../src/async.js')
const {createCommandHandler, maintainCommandHandler} = require('../../../src/command-handler.js')
const {executeCommands} = require('../../../src/command.js')
const {readEvents} = require('../../../src/event.js')
const {configure, inTransaction, UNIQUE_VIOLATION} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('maintainCommandHandler()', () => {
  const nameA = 'aggregate-name-a'
  const instanceA = 'aggregate-instance-a'
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}
  const commandC = {type: commandTypeA, data: 'c'}
  const commandD = {type: commandTypeB, data: 'd'}
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'

  function afterDelay (delay, fn) {
    return new Promise((resolve, reject) => {
      setTimeout(() => {
        fn().then(resolve, reject)
      }, delay)
    })
  }

  function shouldContinueTimes (total) {
    let callCount = 0

    return () => ++callCount < total
  }

  let cancel, context, handleCommand, logger, readAllEvents, restore

  const pgHelper = createTestHelper({
    async beforeEach () {
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      restore = configure()
      pgHelper.trackSchemas('recluse')
      await initializeSchema(context, logger, pgHelper.pool)

      handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [nameA]: {
            commandTypes: [commandTypeA, commandTypeB],
            eventTypes: [eventTypeA, eventTypeB],
            routeCommand: () => instanceA,
            createInitialState: () => null,
            handleCommand: async ({command: {type, data}, recordEvents}) => {
              switch (type) {
                case commandTypeA: return recordEvents({type: eventTypeA, data})
                case commandTypeB: return recordEvents({type: eventTypeB, data})
              }
            },
            applyEvent: () => {},
          },
        },
        {},
      )

      readAllEvents = async () => {
        const events = []

        await pgHelper.inTransaction(async client => {
          await readEvents(context, logger, client, serialization, 0, Infinity, ({event}) => {
            events.push(event)

            return true
          })
        })

        return events
      }
    },

    async afterEach () {
      restore()
      await cancel()
    },
  })

  describe('before handling commands', () => {
    it('should support cancellation', async () => {
      const task = Promise.all([
        maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand),
        cancel(),
      ])

      await expect(task).rejects.toThrow('Canceled')
    })
  })

  describe('while running', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await executeCommands(context, logger, client, serialization, sourceA, [commandA, commandB])
      })
    })

    it('should handle commands', async () => {
      await maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
        shouldContinue: shouldContinueTimes(2),
      })

      expect(await readAllEvents()).toEqual([
        {type: eventTypeA, data: commandA.data},
        {type: eventTypeB, data: commandB.data},
      ])
    })

    it('should throw if commands cannot be handled', async () => {
      const error = new Error('Unique violation')
      error.code = UNIQUE_VIOLATION

      const pool = {
        connect: async () => {
          const client = await pgHelper.pool.connect()

          const clientQuery = client.query.bind(client)
          jest.spyOn(client, 'query').mockImplementation((text, ...args) => {
            if (typeof text === 'string' && text.startsWith('INSERT INTO recluse.stream')) return Promise.reject(error)

            return clientQuery(text, ...args)
          })

          return client
        },
      }

      const task = maintainCommandHandler(context, logger, pool, serialization, handleCommand)

      await expect(task).rejects.toThrow('Unable to handle command-type-a command')
    })

    it('should handle new commands when relying solely on notifications', async () => {
      await Promise.all([
        maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
          shouldContinue: shouldContinueTimes(4),
          timeout: null,
        }),
        pgHelper.inTransaction(async client => {
          await executeCommands(context, logger, client, serialization, sourceA, [commandC, commandD])
        }),
      ])

      expect(await readAllEvents()).toEqual([
        {type: eventTypeA, data: commandA.data},
        {type: eventTypeB, data: commandB.data},
        {type: eventTypeA, data: commandC.data},
        {type: eventTypeB, data: commandD.data},
      ])
    })

    it('should handle new commands when a notification is received before the timeout', async () => {
      await Promise.all([
        maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
          shouldContinue: shouldContinueTimes(4),
          timeout: 100,
        }),
        afterDelay(10, async () => {
          await pgHelper.inTransaction(async client => {
            await executeCommands(context, logger, client, serialization, sourceA, [commandC, commandD])
          })
        }),
      ])

      expect(await readAllEvents()).toEqual([
        {type: eventTypeA, data: commandA.data},
        {type: eventTypeB, data: commandB.data},
        {type: eventTypeA, data: commandC.data},
        {type: eventTypeB, data: commandD.data},
      ])
    })

    it('should handle new commands when the timeout fires before receiving a notification', async () => {
      const executeClient = await pgHelper.pool.connect()
      const executeClientQuery = executeClient.query.bind(executeClient)
      jest.spyOn(executeClient, 'query').mockImplementation((text, ...args) => {
        if (typeof text === 'string' && text.startsWith('NOTIFY')) return Promise.resolve()

        return executeClientQuery(text, ...args)
      })

      try {
        await Promise.all([
          maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
            shouldContinue: shouldContinueTimes(4),
            timeout: 5,
          }),
          afterDelay(10, async () => {
            await inTransaction(context, logger, executeClient, async () => {
              await executeCommands(context, logger, executeClient, serialization, sourceA, [commandC, commandD])
            })
          }),
        ])
      } finally {
        executeClient.release()
      }

      expect(await readAllEvents()).toEqual([
        {type: eventTypeA, data: commandA.data},
        {type: eventTypeB, data: commandB.data},
        {type: eventTypeA, data: commandC.data},
        {type: eventTypeB, data: commandD.data},
      ])
    })
  })

  describe('when resuming handling commands after previous handling', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await executeCommands(context, logger, client, serialization, sourceA, [commandA, commandB])
      })

      await maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
        shouldContinue: shouldContinueTimes(2),
      })
    })

    it('should handle only unhandled commands', async () => {
      await Promise.all([
        maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
          shouldContinue: shouldContinueTimes(2),
        }),
        pgHelper.inTransaction(async client => {
          await executeCommands(context, logger, client, serialization, sourceA, [commandC, commandD])
        }),
      ])

      expect(await readAllEvents()).toEqual([
        {type: eventTypeA, data: commandA.data},
        {type: eventTypeB, data: commandB.data},
        {type: eventTypeA, data: commandC.data},
        {type: eventTypeB, data: commandD.data},
      ])
    })
  })

  describe('when multiple workers try handle commands', () => {
    it('should cooperatively handle commands using a single worker at a time', async () => {
      await Promise.all([
        maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
          shouldContinue: shouldContinueTimes(2),
        }),
        maintainCommandHandler(context, logger, pgHelper.pool, serialization, handleCommand, {
          shouldContinue: shouldContinueTimes(2),
        }),
        pgHelper.inTransaction(async client => {
          const commands = [commandA, commandB, commandC, commandD]
          await executeCommands(context, logger, client, serialization, sourceA, commands)
        }),
      ])

      expect(await readAllEvents()).toEqual([
        {type: eventTypeA, data: commandA.data},
        {type: eventTypeB, data: commandB.data},
        {type: eventTypeA, data: commandC.data},
        {type: eventTypeB, data: commandD.data},
      ])
    })
  })
})
