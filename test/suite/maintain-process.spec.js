const {readCommands} = require('../../src/command.js')
const {appendEvents} = require('../../src/event.js')
const {maintainProcess} = require('../../src/process.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')
const {asyncIterableToArray, consumeAsyncIterable} = require('../helper/async.js')
const {createClock} = require('../helper/clock.js')
const {createTestHelper} = require('../helper/pg.js')

describe('maintainProcess()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    pgHelper.trackSchemas('recluse')
    await initializeSchema(pgHelper.client)
  })

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

  describe('before iteration', () => {
    it('should support cancellation', async () => {
      const process = {
        ...emptyProcess,
      }
      jest.spyOn(process, 'routeEvent')
      await appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 0, [eventA])
      await maintainProcess(serialization, pgHelper.client, nameA, emptyProcess).cancel()

      expect(process.routeEvent).not.toHaveBeenCalled()
    })
  })

  describe('while iterating', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 0, [eventA, eventB])
    })

    it('should process the events in the correct order', async () => {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        createInitialState: () => 0,
        handleEvent: async ({event: {data}, executeCommands, readState, updateState}) => {
          const previousTotal = await readState()
          const total = previousTotal + data

          executeCommands({type: commandTypeA, data: {total, number: data}})
          updateState(total)
        },
      }
      await consumeAsyncIterable(
        maintainProcess(serialization, pgHelper.pool, nameA, process),
        2,
        process => process.cancel(),
        wasProcessed => expect(wasProcessed).toBe(true),
      )
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(2)
      expect(commands[0].command).toEqual({type: commandTypeA, data: {total: 111, number: 111}})
      expect(commands[1].command).toEqual({type: commandTypeA, data: {total: 333, number: 222}})
    })

    it('should update the state only when updateState() is called', async () => {
      await appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 2, [eventC])

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
      await consumeAsyncIterable(
        maintainProcess(serialization, pgHelper.pool, nameA, process),
        3,
        process => process.cancel(),
        wasProcessed => expect(wasProcessed).toBe(true),
      )
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(3)
      expect(commands[0].command).toEqual({type: commandTypeA, data: {total: 111, number: 111}})
      expect(commands[1].command).toEqual({type: commandTypeA, data: {total: 333, number: 222}})
      expect(commands[2].command).toEqual({type: commandTypeA, data: {total: 444, number: 333}})
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
      await consumeAsyncIterable(
        maintainProcess(serialization, pgHelper.pool, nameA, process),
        2,
        process => process.cancel(),
        wasProcessed => expect(wasProcessed).toBe(true),
      )
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(2)
      expect(commands[0].command).toEqual({type: commandTypeA, data: {eventType: eventTypeA}})
      expect(commands[1].command).toEqual({type: commandTypeB, data: {eventType: eventTypeB}})
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
      const expectedWasProcesed = [true, false]
      await consumeAsyncIterable(
        maintainProcess(serialization, pgHelper.pool, nameA, process),
        2,
        process => process.cancel(),
        (wasProcessed, i) => expect(wasProcessed).toBe(expectedWasProcesed[i]),
      )
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(1)
      expect(commands[0].command).toEqual({type: commandTypeA, data: {type: eventTypeA}})
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
      const expectedWasProcesed = [true, false]
      await consumeAsyncIterable(
        maintainProcess(serialization, pgHelper.pool, nameA, process),
        2,
        process => process.cancel(),
        (wasProcessed, i) => expect(wasProcessed).toBe(expectedWasProcesed[i]),
      )
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(1)
      expect(commands[0].command).toEqual({type: commandTypeA, data: {type: eventTypeA}})
    })

    it('should throw if an unexpected commnd is executed', async () => {
      const process = maintainProcess(serialization, pgHelper.pool, nameA, {
        ...emptyProcess,
        eventTypes: [eventTypeA],
        commandTypes: [commandTypeA],
        handleEvent: async ({executeCommands}) => {
          executeCommands({type: commandTypeB})
        },
      })

      await expect(consumeAsyncIterable(process, 1))
        .rejects.toThrow(`Process ${nameA} cannot execute ${commandTypeB} commands`)

      await process.cancel()
    })

    it('should handle errors while processing events', async () => {
      const error = new Error('You done goofed')
      const process = maintainProcess(serialization, pgHelper.pool, nameA, {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        handleEvent: async () => { throw error },
      })

      await expect(consumeAsyncIterable(process, 1)).rejects.toThrow(error)

      await process.cancel()
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
        consumeAsyncIterable(
          maintainProcess(serialization, pgHelper.pool, nameA, process, {timeout: null}),
          4,
          process => process.cancel(),
        ),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(4)
      expect(commands[0].command).toEqual({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).toEqual({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).toEqual({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).toEqual({type: commandTypeA, data: eventD.data})
    })

    it('should process new events when a notification is received before the timeout', async () => {
      const clock = createClock()
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }
      await Promise.all([
        consumeAsyncIterable(
          maintainProcess(serialization, pgHelper.pool, nameA, process, {clock}),
          4,
          process => process.cancel(),
        ),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(4)
      expect(commands[0].command).toEqual({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).toEqual({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).toEqual({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).toEqual({type: commandTypeA, data: eventD.data})
    })

    it('should process new events when the timeout fires before receiving a notification', async () => {
      const clock = createClock({immediate: true})

      const appendClient = await pgHelper.createClient()
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
      await Promise.all([
        consumeAsyncIterable(
          maintainProcess(serialization, pgHelper.pool, nameA, process, {clock}),
          4,
          process => process.cancel(),
        ),

        appendEvents(serialization, appendClient, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(4)
      expect(commands[0].command).toEqual({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).toEqual({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).toEqual({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).toEqual({type: commandTypeA, data: eventD.data})
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

      await appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 0, [eventA, eventB])
      await consumeAsyncIterable(
        maintainProcess(serialization, pgHelper.pool, nameA, process),
        2,
        process => process.cancel(),
      )
    })

    it('should process new events in the correct order', async () => {
      await Promise.all([
        consumeAsyncIterable(
          maintainProcess(serialization, pgHelper.pool, nameA, process, {timeout: null}),
          2,
          process => process.cancel(),
        ),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(4)
      expect(commands[0].command).toEqual({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).toEqual({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).toEqual({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).toEqual({type: commandTypeA, data: eventD.data})
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
      const maintain = () => consumeAsyncIterable(
        maintainProcess(serialization, pgHelper.pool, nameA, process, {timeout: null}),
        2,
        process => process.cancel(),
      )

      await Promise.all([
        maintain(),
        maintain(),

        appendEvents(serialization, pgHelper.client, streamTypeA, streamInstanceA, 0, [eventA, eventB, eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(4)
      expect(commands[0].command).toEqual({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).toEqual({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).toEqual({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).toEqual({type: commandTypeA, data: eventD.data})
    })
  })
})
