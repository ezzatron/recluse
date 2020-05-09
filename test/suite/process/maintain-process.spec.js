const {createContext} = require('../../../src/async.js')
const {readCommands} = require('../../../src/command.js')
const {appendEvents} = require('../../../src/event.js')
const {configure, inTransaction} = require('../../../src/pg.js')
const {maintainProcess} = require('../../../src/process.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('maintainProcess()', () => {
  const nameA = 'process-name-a'
  const instanceA = 'process-instance-a'
  const streamTypeA = 'stream-type-a'
  const streamInstanceA = 'stream-instance-a'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const eventA = {type: eventTypeA, data: 111}
  const eventB = {type: eventTypeB, data: 222}
  const eventC = {type: eventTypeA, data: 333}
  const eventD = {type: eventTypeB, data: 444}

  const emptyProcess = {
    eventTypes: [],
    commandTypes: [],
    routeEvent: () => instanceA,
    createInitialState: () => null,
    handleEvent: () => {},
  }

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

  let cancel, context, logger, readAllCommands, restore

  const pgHelper = createTestHelper({
    async beforeEach () {
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      restore = configure()
      pgHelper.trackSchemas('recluse')
      await initializeSchema(context, logger, pgHelper.pool)

      readAllCommands = async () => {
        const commands = []

        await pgHelper.inTransaction(async client => {
          await readCommands(context, logger, client, serialization, 0, ({command}) => {
            commands.push(command)

            return true
          })
        })

        return commands
      }
    },

    afterEach () {
      restore && restore()
      cancel && cancel()
    },
  })

  describe('before processing events', () => {
    it('should support cancellation', async () => {
      const task = Promise.all([
        maintainProcess(context, logger, pgHelper.pool, serialization, nameA, emptyProcess),
        cancel(),
      ])

      await expect(task).rejects.toThrow('Canceled')
    })
  })

  describe('while running', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 0, [eventA, eventB])
      })
    })

    it('should process the events in the correct order', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        createInitialState: () => 0,
        async handleEvent ({event: {data}, executeCommands, readState, updateState}) {
          const previousTotal = await readState()
          const total = previousTotal + data

          executeCommands({type: commandTypeA, data: {total, number: data}})
          updateState(total)
        },
      }
      await maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
        shouldContinue: shouldContinueTimes(2),
      })

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: {total: 111, number: 111}},
        {type: commandTypeA, data: {total: 333, number: 222}},
      ])
    })

    it('should update the state only when updateState() is called', async () => {
      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 2, [eventC])
      })

      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        createInitialState: () => ({total: 0}),
        handleEvent: async ({event: {data, type}, executeCommands, readState, updateState}) => {
          const state = await readState()
          const total = state.total + data

          executeCommands({type: commandTypeA, data: {total, number: data}})

          if (type === eventTypeA) {
            updateState({total})
          } else {
            state.total = total
          }
        },
      }
      await maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
        shouldContinue: shouldContinueTimes(3),
      })

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: {total: 111, number: 111}},
        {type: commandTypeA, data: {total: 333, number: 222}},
        {type: commandTypeA, data: {total: 444, number: 333}},
      ])
    })

    it('should process different event types', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA, commandTypeB],
        handleEvent: async ({event: {type: eventType}, executeCommands}) => {
          const type = eventType === eventTypeA ? commandTypeA : commandTypeB

          executeCommands({type, data: {eventType}})
        },
      }
      await maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
        shouldContinue: shouldContinueTimes(2),
      })

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: {eventType: eventTypeA}},
        {type: commandTypeB, data: {eventType: eventTypeB}},
      ])
    })

    it('should ignore event types that should not be processed', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {type}, executeCommands}) => {
          executeCommands({type: commandTypeA, data: {type}})
        },
      }
      await maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
        shouldContinue: shouldContinueTimes(2),
      })

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: {type: eventTypeA}},
      ])
    })

    it('should ignore event types that do not route to a process instance', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        routeEvent: ({type}) => type === eventTypeA ? instanceA : null,
        handleEvent: async ({event: {type}, executeCommands}) => {
          executeCommands({type: commandTypeA, data: {type}})
        },
      }
      await maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
        shouldContinue: shouldContinueTimes(2),
      })

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: {type: eventTypeA}},
      ])
    })

    it('should throw if an unexpected commnd is executed', async () => {
      const task = maintainProcess(context, logger, pgHelper.pool, serialization, nameA, {
        ...emptyProcess,
        eventTypes: [eventTypeA],
        commandTypes: [commandTypeA],
        handleEvent: async ({executeCommands}) => {
          executeCommands({type: commandTypeB})
        },
      })

      await expect(task).rejects.toThrow(`Process ${nameA} cannot execute ${commandTypeB} commands`)
    })

    it('should handle errors while processing events', async () => {
      const error = new Error('You done goofed')
      const task = maintainProcess(context, logger, pgHelper.pool, serialization, nameA, {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        handleEvent: async () => { throw error },
      })

      await expect(task).rejects.toThrow(error)
    })

    it('should be able to process new events when relying solely on notifications', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }
      await Promise.all([
        maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
          shouldContinue: shouldContinueTimes(4),
          timeout: null,
        }),
        pgHelper.inTransaction(async client => {
          await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 2, [eventC, eventD])
        }),
      ])

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: eventA.data},
        {type: commandTypeA, data: eventB.data},
        {type: commandTypeA, data: eventC.data},
        {type: commandTypeA, data: eventD.data},
      ])
    })

    it('should process new events when a notification is received before the timeout', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }
      await Promise.all([
        maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
          shouldContinue: shouldContinueTimes(4),
          timeout: 100,
        }),
        afterDelay(10, async () => {
          await pgHelper.inTransaction(async client => {
            const events = [eventC, eventD]
            await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 2, events)
          })
        }),
      ])

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: eventA.data},
        {type: commandTypeA, data: eventB.data},
        {type: commandTypeA, data: eventC.data},
        {type: commandTypeA, data: eventD.data},
      ])
    })

    it('should process new events when the timeout fires before receiving a notification', async () => {
      const appendClient = await pgHelper.pool.connect()
      const appendClientQuery = appendClient.query.bind(appendClient)
      jest.spyOn(appendClient, 'query').mockImplementation((text, ...args) => {
        if (typeof text === 'string' && text.startsWith('NOTIFY')) return Promise.resolve()

        return appendClientQuery(text, ...args)
      })

      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }

      try {
        await Promise.all([
          maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
            shouldContinue: shouldContinueTimes(4),
            timeout: 5,
          }),
          afterDelay(10, async () => {
            await inTransaction(context, logger, appendClient, async () => {
              const events = [eventC, eventD]
              await appendEvents(context, logger, appendClient, serialization, streamTypeA, streamInstanceA, 2, events)
            })
          }),
        ])
      } finally {
        appendClient.release()
      }

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: eventA.data},
        {type: commandTypeA, data: eventB.data},
        {type: commandTypeA, data: eventC.data},
        {type: commandTypeA, data: eventD.data},
      ])
    })
  })

  describe('when resuming the maintenance of an existing process', () => {
    let process

    beforeEach(async () => {
      process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }

      await pgHelper.inTransaction(async client => {
        await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 0, [eventA, eventB])
      })

      await maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
        shouldContinue: shouldContinueTimes(2),
      })
    })

    it('should process new events in the correct order', async () => {
      await Promise.all([
        maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
          shouldContinue: shouldContinueTimes(2),
        }),
        pgHelper.inTransaction(async client => {
          await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 2, [eventC, eventD])
        }),
      ])

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: eventA.data},
        {type: commandTypeA, data: eventB.data},
        {type: commandTypeA, data: eventC.data},
        {type: commandTypeA, data: eventD.data},
      ])
    })
  })

  describe('when multiple workers try to maintain the same process', () => {
    it('should cooperatively process events using a single worker at a time', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }
      await Promise.all([
        maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
          shouldContinue: shouldContinueTimes(2),
        }),
        maintainProcess(context, logger, pgHelper.pool, serialization, nameA, process, {
          shouldContinue: shouldContinueTimes(2),
        }),
        pgHelper.inTransaction(async client => {
          const events = [eventA, eventB, eventC, eventD]
          await appendEvents(context, logger, client, serialization, streamTypeA, streamInstanceA, 0, events)
        }),
      ])

      expect(await readAllCommands()).toEqual([
        {type: commandTypeA, data: eventA.data},
        {type: commandTypeA, data: eventB.data},
        {type: commandTypeA, data: eventC.data},
        {type: commandTypeA, data: eventD.data},
      ])
    })
  })
})
