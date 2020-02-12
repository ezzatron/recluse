const {expect} = require('chai')

const {asyncIterableToArray, pgSpec} = require('../helper.js')

const {createCommandHandler} = require('../../src/command-handler.js')
const {initializeSchema} = require('../../src/schema.js')
const {readEventsByStream} = require('../../src/event.js')
const {serialization} = require('../../src/serialization/json.js')

describe('handleCommand()', pgSpec(function () {
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

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('when creating', function () {
    it('should throw when the serialization is not supplied', async function () {
      const operation = () => createCommandHandler()

      expect(operation).to.throw('Invalid serialization')
    })

    it('should throw when aggregates are not supplied', async function () {
      const operation = () => createCommandHandler(serialization)

      expect(operation).to.throw('Invalid aggregates')
    })

    it('should throw when integrations are not supplied', async function () {
      const operation = () => createCommandHandler(serialization, {})

      expect(operation).to.throw('Invalid integrations')
    })

    it('should throw when multiple aggregates attempt to handle the same command type', async function () {
      const operation = () => createCommandHandler(
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

      expect(operation).to.throw('already handled')
    })

    it('should throw when multiple integrations attempt to handle the same command type', async function () {
      const operation = () => createCommandHandler(
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

      expect(operation).to.throw('already handled')
    })

    it('should throw when aggregates and integrations attempt to handle the same command type', async function () {
      const operation = () => createCommandHandler(
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

      expect(operation).to.throw('already handled')
    })
  })

  context('when handling commands', function () {
    it('should throw when handling commands with unexpected types', async function () {
      const handleCommand = createCommandHandler(
        serialization,
        {
          [aggregateNameA]: emptyAggregate,
        },
        {
          [integrationNameA]: emptyIntegration,
        },
      )

      const operation = this.inTransaction(async () => {
        await handleCommand(this.pgClient, {type: commandTypeA})
      })

      await expect(operation).to.be.rejectedWith(`Unable to handle ${commandTypeA} command - no suitable handler found`)
    })

    it('should throw when handling commands that cannot be routed', async function () {
      const handleCommand = createCommandHandler(
        serialization,
        {
          [aggregateNameA]: {
            ...emptyAggregate,
            commandTypes: [commandTypeA],
          },
        },
        {},
      )

      const operation = this.inTransaction(async () => {
        await handleCommand(this.pgClient, {type: commandTypeA})
      })

      await expect(operation).to.be.rejectedWith(`Unable to handle ${commandTypeA} command - no suitable route found`)
    })
  })

  context('when handling commands with aggregates', function () {
    it('should be able to handle commands', async function () {
      const createCommandA = increment => ({type: commandTypeA, data: {increment}})
      const createCommandB = increment => ({type: commandTypeB, data: {increment}})
      const instance = 'aggregate-instance-a'
      const handleCommand = createCommandHandler(
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

      const isHandled = []
      await this.inTransaction(async () => {
        isHandled.push(await handleCommand(this.pgClient, createCommandA(111)))
        isHandled.push(await handleCommand(this.pgClient, createCommandA(222)))
        isHandled.push(await handleCommand(this.pgClient, createCommandB(333)))
      })

      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, aggregateStreamA, instance))

      expect(isHandled).to.deep.equal([true, true, true])
      expect(events).to.have.length(3)
      expect(events[0].event).to.deep.equal({type: eventTypeA, data: {current: 0, increment: 111}})
      expect(events[1].event).to.deep.equal({type: eventTypeA, data: {current: 111, increment: 222}})
      expect(events[2].event).to.deep.equal({type: eventTypeB, data: {current: 0, increment: 333}})
    })

    it('should immediately apply recorded events to the state', async function () {
      const createCommandA = () => ({type: commandTypeA})
      const instance = 'aggregate-instance-a'
      const handleCommand = createCommandHandler(
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

      const isHandled = await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA()))
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, aggregateStreamA, instance))

      expect(isHandled).to.be.true()
      expect(events).to.have.length(2)
      expect(events[0].event).to.deep.equal({type: eventTypeA, data: {value: 111}})
      expect(events[1].event).to.deep.equal({type: eventTypeA, data: {value: 222}})
    })

    it('should not allow failures to affect the state for future calls', async function () {
      const createCommandA = isOkay => ({type: commandTypeA, data: {isOkay}})
      const instance = 'aggregate-instance-a'
      const notOkay = new Error('Not okay')
      const handleCommand = createCommandHandler(
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

      const isHandled = []
      let error
      isHandled.push(await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA(true))))
      try {
        await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA(false)))
      } catch (e) {
        error = e
      }
      isHandled.push(await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA(true))))
      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, aggregateStreamA, instance))

      expect(isHandled).to.deep.equal([true, true])
      expect(error).to.equal(notOkay)
      expect(events).to.have.length(2)
      expect(events[0].event).to.deep.equal({type: eventTypeA, data: {value: 111}})
      expect(events[1].event).to.deep.equal({type: eventTypeA, data: {value: 222}})
    })

    it('should be able to route to instances on a per-command basis', async function () {
      const createCommandA = (instance, increment) => ({type: commandTypeA, data: {instance, increment}})
      const instanceA = 'aggregate-instance-a'
      const instanceB = 'aggregate-instance-b'
      const handleCommand = createCommandHandler(
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

            applyEvent: async ({event: {type, data: {increment}}, updateState}) => {
              await updateState(state => state + increment)
            },
          },
        },
        {},
      )

      const isHandled = []
      await this.inTransaction(async () => {
        isHandled.push(await handleCommand(this.pgClient, createCommandA(instanceA, 111)))
        isHandled.push(await handleCommand(this.pgClient, createCommandA(instanceB, 222)))
        isHandled.push(await handleCommand(this.pgClient, createCommandA(instanceA, 333)))
        isHandled.push(await handleCommand(this.pgClient, createCommandA(instanceB, 444)))
      })

      const [eventsA] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, aggregateStreamA, instanceA))
      const [eventsB] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, aggregateStreamA, instanceB))

      expect(isHandled).to.deep.equal([true, true, true, true])
      expect(eventsA).to.have.length(2)
      expect(eventsA[0].event).to.deep.equal({type: eventTypeA, data: {current: 0, increment: 111}})
      expect(eventsA[1].event).to.deep.equal({type: eventTypeA, data: {current: 111, increment: 333}})
      expect(eventsB).to.have.length(2)
      expect(eventsB[0].event).to.deep.equal({type: eventTypeA, data: {current: 0, increment: 222}})
      expect(eventsB[1].event).to.deep.equal({type: eventTypeA, data: {current: 222, increment: 444}})
    })

    it('should support multiple handlers', async function () {
      const createCommandA = () => ({type: commandTypeA})
      const createCommandB = () => ({type: commandTypeB})
      const instanceA = 'aggregate-instance-a'
      const instanceB = 'aggregate-instance-b'
      const handleCommand = createCommandHandler(
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

      const isHandled = []
      await this.inTransaction(async () => {
        isHandled.push(await handleCommand(this.pgClient, createCommandA()))
        isHandled.push(await handleCommand(this.pgClient, createCommandB()))
        isHandled.push(await handleCommand(this.pgClient, createCommandA()))
        isHandled.push(await handleCommand(this.pgClient, createCommandB()))
      })

      const [eventsA] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, aggregateStreamA, instanceA))
      const [eventsB] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, aggregateStreamB, instanceB))

      expect(isHandled).to.deep.equal([true, true, true, true])
      expect(eventsA).to.have.length(2)
      expect(eventsA[0].event).to.deep.equal({type: eventTypeA, data: 0})
      expect(eventsA[1].event).to.deep.equal({type: eventTypeA, data: 1})
      expect(eventsB).to.have.length(2)
      expect(eventsB[0].event).to.deep.equal({type: eventTypeA, data: 0})
      expect(eventsB[1].event).to.deep.equal({type: eventTypeA, data: 1})
    })

    it('should throw when recording events with unexpected types', async function () {
      const handleCommand = createCommandHandler(
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

      const operation = this.inTransaction(async () => {
        await handleCommand(this.pgClient, {type: commandTypeA})
      })

      await expect(operation).to.be.rejectedWith(`Aggregate ${aggregateNameA} cannot record ${eventTypeA} events`)
    })
  })

  context('when handling commands with integrations', function () {
    it('should be able to handle commands', async function () {
      const createCommandA = data => ({type: commandTypeA, data})
      const createCommandB = data => ({type: commandTypeB, data})
      const handleCommand = createCommandHandler(
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

      const isHandled = []
      await this.inTransaction(async () => {
        isHandled.push(await handleCommand(this.pgClient, createCommandA(111)))
        isHandled.push(await handleCommand(this.pgClient, createCommandA(222)))
        isHandled.push(await handleCommand(this.pgClient, createCommandB(333)))
      })

      const [events] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, integrationStreamA, ''))

      expect(isHandled).to.deep.equal([true, true, true])
      expect(events).to.have.length(3)
      expect(events[0].event).to.deep.equal({type: eventTypeA, data: 111})
      expect(events[1].event).to.deep.equal({type: eventTypeA, data: 222})
      expect(events[2].event).to.deep.equal({type: eventTypeB, data: 333})
    })

    it('should support multiple handlers', async function () {
      const createCommandA = data => ({type: commandTypeA, data})
      const createCommandB = data => ({type: commandTypeB, data})
      const handleCommand = createCommandHandler(
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

      const isHandled = []
      await this.inTransaction(async () => {
        isHandled.push(await handleCommand(this.pgClient, createCommandA(111)))
        isHandled.push(await handleCommand(this.pgClient, createCommandB(222)))
        isHandled.push(await handleCommand(this.pgClient, createCommandA(333)))
        isHandled.push(await handleCommand(this.pgClient, createCommandB(444)))
      })

      const [eventsA] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, integrationStreamA, ''))
      const [eventsB] =
        await asyncIterableToArray(readEventsByStream(serialization, this.pgClient, integrationStreamB, ''))

      expect(isHandled).to.deep.equal([true, true, true, true])
      expect(eventsA).to.have.length(2)
      expect(eventsA[0].event).to.deep.equal({type: eventTypeA, data: 111})
      expect(eventsA[1].event).to.deep.equal({type: eventTypeA, data: 333})
      expect(eventsB).to.have.length(2)
      expect(eventsB[0].event).to.deep.equal({type: eventTypeA, data: 222})
      expect(eventsB[1].event).to.deep.equal({type: eventTypeA, data: 444})
    })

    it('should throw when recording events with unexpected types', async function () {
      const handleCommand = createCommandHandler(
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

      const operation = this.inTransaction(async () => {
        await handleCommand(this.pgClient, {type: commandTypeA})
      })

      await expect(operation).to.be.rejectedWith(`Integration ${integrationNameA} cannot record ${eventTypeA} events`)
    })
  })
}))
