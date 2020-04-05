const {createContext} = require('../../../src/async.js')
const {createCommandHandler} = require('../../../src/command-handler.js')
const {readEventsByStream} = require('../../../src/event.js')
const {configure} = require('../../../src/pg.js')
const {initializeSchema} = require('../../../src/schema.js')
const {serialization} = require('../../../src/serialization/json.js')
const {createLogger} = require('../../helper/logging.js')
const {createTestHelper} = require('../../helper/pg.js')

describe('handleCommand()', () => {
  const aggregateNameA = 'aggregate-name-a'
  const aggregateNameB = 'aggregate-name-b'
  const aggregateStreamA = `aggregate.${aggregateNameA}`
  const aggregateStreamB = `aggregate.${aggregateNameB}`
  const integrationNameA = 'integration-name-a'
  const integrationNameB = 'integration-name-b'
  const integrationStreamA = `integration.${integrationNameA}`
  const integrationStreamB = `integration.${integrationNameB}`
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'

  const emptyAggregate = {
    commandTypes: [],
    eventTypes: [],
    routeCommand: () => {},
    createInitialState: () => {},
    handleCommand: () => {},
    applyEvent: () => {},
  }

  const emptyIntegration = {
    commandTypes: [],
    eventTypes: [],
    handleCommand: () => {},
  }

  let cancel, context, logger, readAllEventsByStream, restore

  const pgHelper = createTestHelper({
    async beforeEach () {
      logger = createLogger()

      const created = createContext(logger)
      context = created[0]
      cancel = created[1]

      restore = configure()
      pgHelper.trackSchemas('recluse')
      await initializeSchema(context, logger, pgHelper.pool)

      readAllEventsByStream = async (client, type, instance) => {
        const events = []

        await readEventsByStream(context, logger, client, serialization, type, instance, 0, Infinity, ({event}) => {
          events.push(event)

          return true
        })

        return events
      }
    },

    async afterEach () {
      restore()
      await cancel()
    },
  })

  describe('when creating', () => {
    it('should throw when the logger is not supplied', () => {
      const operation = () => createCommandHandler()

      expect(operation).toThrow('Invalid logger')
    })

    it('should throw when the serialization is not supplied', () => {
      const operation = () => createCommandHandler(logger)

      expect(operation).toThrow('Invalid serialization')
    })

    it('should throw when aggregates are not supplied', () => {
      const operation = () => createCommandHandler(logger, serialization)

      expect(operation).toThrow('Invalid aggregates')
    })

    it('should throw when integrations are not supplied', () => {
      const operation = () => createCommandHandler(logger, serialization, {})

      expect(operation).toThrow('Invalid integrations')
    })

    it('should throw when multiple aggregates attempt to handle the same command type', () => {
      const operation = () => createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
          },

          [aggregateNameB]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
          },
        },
        {},
      )

      expect(operation).toThrow('already handled')
    })

    it('should throw when multiple integrations attempt to handle the same command type', () => {
      const operation = () => createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
          },
        },
        {
          [integrationNameA]: {
            ...emptyIntegration,
            commandTypes: [commandTypeA],
          },
        },
      )

      expect(operation).toThrow('already handled')
    })

    it('should throw when aggregates and integrations attempt to handle the same command type', () => {
      const operation = () => createCommandHandler(
        logger,
        serialization,
        {},
        {
          [integrationNameA]: {
            ...emptyIntegration,
            commandTypes: [commandTypeA],
          },

          [integrationNameB]: {
            ...emptyIntegration,
            commandTypes: [commandTypeA],
          },
        },
      )

      expect(operation).toThrow('already handled')
    })
  })

  describe('when handling commands', () => {
    it('should throw when handling commands with unexpected types', async () => {
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: emptyAggregate,
        },
        {
          [integrationNameA]: emptyIntegration,
        },
      )

      const operation = pgHelper.inTransaction(async client => {
        await handleCommand(context, client, {type: commandTypeA})
      })

      await expect(operation).rejects.toThrow(`Unable to handle ${commandTypeA} command - no suitable handler found`)
    })

    it('should throw when handling commands that cannot be routed', async () => {
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
          },
        },
        {},
      )

      const operation = pgHelper.inTransaction(async client => {
        await handleCommand(context, client, {type: commandTypeA})
      })

      await expect(operation).rejects.toThrow(`Unable to handle ${commandTypeA} command - no suitable route found`)
    })
  })

  describe('when handling commands with aggregates', () => {
    it('should be able to handle commands', async () => {
      const createCommandA = increment => ({type: commandTypeA, data: {increment}})
      const createCommandB = increment => ({type: commandTypeB, data: {increment}})
      const instance = 'aggregate-instance-a'
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA, commandTypeB],
            eventTypes: [eventTypeA, eventTypeB],
            routeCommand: () => instance,
            createInitialState: () => ({a: 0, b: 0}),

            handleCommand: async ({command: {type, data: {increment}}, readState, recordEvents}) => {
              const {a, b} = await readState()

              switch (type) {
                case commandTypeA: return recordEvents({type: eventTypeA, data: {current: a, increment}})
                case commandTypeB: return recordEvents({type: eventTypeB, data: {current: b, increment}})
              }
            },

            applyEvent: async ({event: {type, data: {increment}}, updateState}) => {
              switch (type) {
                case eventTypeA: return updateState(state => { state.a += increment })
                case eventTypeB: return updateState(state => { state.b += increment })
              }
            },
          },
        },
        {},
      )

      await pgHelper.inTransaction(async client => {
        const isHandled = []
        isHandled.push(await handleCommand(context, client, createCommandA(111)))
        isHandled.push(await handleCommand(context, client, createCommandA(222)))
        isHandled.push(await handleCommand(context, client, createCommandB(333)))

        const events = await readAllEventsByStream(client, aggregateStreamA, instance)

        expect(isHandled).toEqual([true, true, true])
        expect(events).toEqual([
          {type: eventTypeA, data: {current: 0, increment: 111}},
          {type: eventTypeA, data: {current: 111, increment: 222}},
          {type: eventTypeB, data: {current: 0, increment: 333}},
        ])
      })
    })

    it('should immediately apply recorded events to the state', async () => {
      const createCommandA = () => ({type: commandTypeA})
      const instance = 'aggregate-instance-a'
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
            eventTypes: [eventTypeA],
            routeCommand: () => instance,
            createInitialState: () => 0,

            handleCommand: async ({readState, recordEvents}) => {
              let state

              state = await readState()
              await recordEvents({type: eventTypeA, data: {value: state + 111}})

              state = await readState()
              await recordEvents({type: eventTypeA, data: {value: state + 111}})
            },

            applyEvent: async ({event: {data: {value}}, updateState}) => {
              await updateState(value)
            },
          },
        },
        {},
      )

      await pgHelper.inTransaction(async client => {
        const isHandled = await handleCommand(context, client, createCommandA())
        const events = await readAllEventsByStream(client, aggregateStreamA, instance)

        expect(isHandled).toBe(true)
        expect(events).toEqual([
          {type: eventTypeA, data: {value: 111}},
          {type: eventTypeA, data: {value: 222}},
        ])
      })
    })

    it('should not allow failures to affect the state for future calls', async () => {
      const createCommandA = isOkay => ({type: commandTypeA, data: {isOkay}})
      const instance = 'aggregate-instance-a'
      const notOkay = new Error('Not okay')
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
            eventTypes: [eventTypeA],
            routeCommand: () => instance,
            createInitialState: () => 0,

            handleCommand: async ({command: {data: {isOkay}}, readState, recordEvents}) => {
              const state = await readState()
              await recordEvents({type: eventTypeA, data: {value: state + 111}})

              if (!isOkay) throw notOkay
            },

            applyEvent: async ({event: {data: {value}}, updateState}) => {
              updateState(value)
            },
          },
        },
        {},
      )

      await pgHelper.inTransaction(async client => {
        const isHandled = []
        let error

        isHandled.push(await handleCommand(context, client, createCommandA(true)))

        try {
          await handleCommand(context, client, createCommandA(false))
        } catch (e) {
          error = e
        }

        isHandled.push(await handleCommand(context, client, createCommandA(true)))

        const events = await readAllEventsByStream(client, aggregateStreamA, instance)

        expect(error).toBe(notOkay)
        expect(isHandled).toEqual([true, true])
        expect(events).toEqual([
          {type: eventTypeA, data: {value: 111}},
          {type: eventTypeA, data: {value: 222}},
        ])
      })
    })

    it('should be able to route to instances on a per-command basis', async () => {
      const createCommandA = (instance, increment) => ({type: commandTypeA, data: {instance, increment}})
      const instanceA = 'aggregate-instance-a'
      const instanceB = 'aggregate-instance-b'
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
            eventTypes: [eventTypeA],
            routeCommand: ({data: {instance}}) => instance,
            createInitialState: () => 0,

            handleCommand: async ({command: {data: {increment}}, readState, recordEvents}) => {
              const state = await readState()
              await recordEvents({type: eventTypeA, data: {current: state, increment}})
            },

            applyEvent: async ({event: {data: {increment}}, updateState}) => {
              await updateState(state => state + increment)
            },
          },
        },
        {},
      )

      await pgHelper.inTransaction(async client => {
        const isHandled = []
        isHandled.push(await handleCommand(context, client, createCommandA(instanceA, 111)))
        isHandled.push(await handleCommand(context, client, createCommandA(instanceB, 222)))
        isHandled.push(await handleCommand(context, client, createCommandA(instanceA, 333)))
        isHandled.push(await handleCommand(context, client, createCommandA(instanceB, 444)))

        const eventsA = await readAllEventsByStream(client, aggregateStreamA, instanceA)
        const eventsB = await readAllEventsByStream(client, aggregateStreamA, instanceB)

        expect(isHandled).toEqual([true, true, true, true])
        expect(eventsA).toEqual([
          {type: eventTypeA, data: {current: 0, increment: 111}},
          {type: eventTypeA, data: {current: 111, increment: 333}},
        ])
        expect(eventsB).toEqual([
          {type: eventTypeA, data: {current: 0, increment: 222}},
          {type: eventTypeA, data: {current: 222, increment: 444}},
        ])
      })
    })

    it('should support multiple handlers', async () => {
      const createCommandA = () => ({type: commandTypeA})
      const createCommandB = () => ({type: commandTypeB})
      const instanceA = 'aggregate-instance-a'
      const instanceB = 'aggregate-instance-b'
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
            eventTypes: [eventTypeA],
            routeCommand: () => instanceA,
            createInitialState: () => 0,
            handleCommand: async ({readState, recordEvents}) => {
              const state = await readState()
              await recordEvents({type: eventTypeA, data: state})
            },
            applyEvent: async ({updateState}) => updateState(state => ++state),
          },

          [aggregateNameB]: {
            ...emptyAggregate,
            commandTypes: [commandTypeB],
            eventTypes: [eventTypeA],
            routeCommand: () => instanceB,
            createInitialState: () => 0,
            handleCommand: async ({readState, recordEvents}) => {
              const state = await readState()
              await recordEvents({type: eventTypeA, data: state})
            },
            applyEvent: async ({updateState}) => updateState(state => ++state),
          },
        },
        {},
      )

      await pgHelper.inTransaction(async client => {
        const isHandled = []
        isHandled.push(await handleCommand(context, client, createCommandA()))
        isHandled.push(await handleCommand(context, client, createCommandB()))
        isHandled.push(await handleCommand(context, client, createCommandA()))
        isHandled.push(await handleCommand(context, client, createCommandB()))

        const eventsA = await readAllEventsByStream(client, aggregateStreamA, instanceA)
        const eventsB = await readAllEventsByStream(client, aggregateStreamB, instanceB)

        expect(isHandled).toEqual([true, true, true, true])
        expect(eventsA).toEqual([
          {type: eventTypeA, data: 0},
          {type: eventTypeA, data: 1},
        ])
        expect(eventsB).toEqual([
          {type: eventTypeA, data: 0},
          {type: eventTypeA, data: 1},
        ])
      })
    })

    it('should throw when recording events with unexpected types', async () => {
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
            routeCommand: () => 'aggregate-instance-a',
            handleCommand: async ({recordEvents}) => recordEvents({type: eventTypeA}),
          },
        },
        {},
      )

      const operation = pgHelper.inTransaction(async client => {
        await handleCommand(context, client, {type: commandTypeA})
      })

      await expect(operation).rejects.toThrow(`Aggregate ${aggregateNameA} cannot record ${eventTypeA} events`)
    })
  })

  describe('when handling commands with integrations', () => {
    it('should be able to handle commands', async () => {
      const createCommandA = data => ({type: commandTypeA, data})
      const createCommandB = data => ({type: commandTypeB, data})
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {},
        {
          [integrationNameA]: {
            ...emptyIntegration,
            commandTypes: [commandTypeA, commandTypeB],
            eventTypes: [eventTypeA, eventTypeB],

            handleCommand: async ({command: {type, data}, recordEvents}) => {
              switch (type) {
                case commandTypeA: return recordEvents({type: eventTypeA, data})
                case commandTypeB: return recordEvents({type: eventTypeB, data})
              }
            },
          },
        },
      )

      await pgHelper.inTransaction(async client => {
        const isHandled = []
        isHandled.push(await handleCommand(context, client, createCommandA(111)))
        isHandled.push(await handleCommand(context, client, createCommandA(222)))
        isHandled.push(await handleCommand(context, client, createCommandB(333)))

        const events = await readAllEventsByStream(client, integrationStreamA, '')

        expect(isHandled).toEqual([true, true, true])
        expect(events).toEqual([
          {type: eventTypeA, data: 111},
          {type: eventTypeA, data: 222},
          {type: eventTypeB, data: 333},
        ])
      })
    })

    it('should support multiple handlers', async () => {
      const createCommandA = data => ({type: commandTypeA, data})
      const createCommandB = data => ({type: commandTypeB, data})
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {},
        {
          [integrationNameA]: {
            ...emptyIntegration,
            commandTypes: [commandTypeA],
            eventTypes: [eventTypeA],
            handleCommand: async ({command: {data}, recordEvents}) => {
              await recordEvents({type: eventTypeA, data})
            },
          },

          [integrationNameB]: {
            ...emptyIntegration,
            commandTypes: [commandTypeB],
            eventTypes: [eventTypeA],
            handleCommand: async ({command: {data}, recordEvents}) => {
              await recordEvents({type: eventTypeA, data})
            },
          },
        },
      )

      await pgHelper.inTransaction(async client => {
        const isHandled = []
        isHandled.push(await handleCommand(context, client, createCommandA(111)))
        isHandled.push(await handleCommand(context, client, createCommandB(222)))
        isHandled.push(await handleCommand(context, client, createCommandA(333)))
        isHandled.push(await handleCommand(context, client, createCommandB(444)))

        const eventsA = await readAllEventsByStream(client, integrationStreamA, '')
        const eventsB = await readAllEventsByStream(client, integrationStreamB, '')

        expect(isHandled).toEqual([true, true, true, true])
        expect(eventsA).toEqual([
          {type: eventTypeA, data: 111},
          {type: eventTypeA, data: 333},
        ])
        expect(eventsB).toEqual([
          {type: eventTypeA, data: 222},
          {type: eventTypeA, data: 444},
        ])
      })
    })

    it('should throw when recording events with unexpected types', async () => {
      const handleCommand = createCommandHandler(
        logger,
        serialization,
        {},
        {
          [integrationNameA]: {
            ...emptyIntegration,
            commandTypes: [commandTypeA],
            handleCommand: async ({recordEvents}) => recordEvents({type: eventTypeA}),
          },
        },
        {},
      )

      const operation = pgHelper.inTransaction(async client => {
        await handleCommand(context, client, {type: commandTypeA})
      })

      await expect(operation).rejects.toThrow(`Integration ${integrationNameA} cannot record ${eventTypeA} events`)
    })
  })
})
