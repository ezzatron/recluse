const {expect} = require('chai')

const {asyncIterableToArray, jsonBuffer, parseJsonBuffer, pgSpec} = require('../helper.js')

const {createCommandHandler} = require('../../src/command.js')
const {initializeSchema} = require('../../src/schema.js')
const {readEventsByStream} = require('../../src/event.js')

describe('handleCommand()', pgSpec(function () {
  const nameA = 'aggregate-name-a'
  const nameB = 'aggregate-name-b'
  const typeA = 'aggregate-type-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'

  const emptyAggregate = {
    type: typeA,
    commandTypes: [],
    eventTypes: [],
    routeCommand: () => {},
    createInitialState: () => {},
    handleCommand: () => {},
    applyEvent: () => {},
  }

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  it('should be able to handle commands', async function () {
    const createCommandA = increment => ({type: commandTypeA, data: {increment}})
    const createCommandB = increment => ({type: commandTypeB, data: {increment}})
    const id = 'aggregate-id-a'
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA, commandTypeB],
        eventTypes: [eventTypeA, eventTypeB],
        routeCommand: () => id,
        createInitialState: () => ({a: 0, b: 0}),

        handleCommand: ({state: {a, b}, command: {type, data: {increment}}, recordEvents}) => {
          switch (type) {
            case commandTypeA: return recordEvents({type: eventTypeA, data: jsonBuffer({current: a, increment})})
            case commandTypeB: return recordEvents({type: eventTypeB, data: jsonBuffer({current: b, increment})})
          }
        },

        applyEvent: (state, {type, data}) => {
          const {increment} = parseJsonBuffer(data)

          switch (type) {
            case eventTypeA: return (state.a += increment)
            case eventTypeB: return (state.b += increment)
          }
        },
      },
    })

    const isHandled = []
    await this.inTransaction(async () => {
      isHandled.push(await handleCommand(this.pgClient, createCommandA(111)))
      isHandled.push(await handleCommand(this.pgClient, createCommandA(222)))
      isHandled.push(await handleCommand(this.pgClient, createCommandB(333)))
    })

    const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, id))

    expect(isHandled).to.deep.equal([true, true, true])
    expect(events).to.have.length(3)
    expect(events[0].event).to.have.fields({type: eventTypeA, data: jsonBuffer({current: 0, increment: 111})})
    expect(events[1].event).to.have.fields({type: eventTypeA, data: jsonBuffer({current: 111, increment: 222})})
    expect(events[2].event).to.have.fields({type: eventTypeB, data: jsonBuffer({current: 0, increment: 333})})
  })

  it('should create aggregate streams with the appropriate type', async function () {
    const createCommandA = () => ({type: commandTypeA})
    const id = 'aggregate-id-a'
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA],
        eventTypes: [eventTypeA],
        routeCommand: () => id,
        handleCommand: ({recordEvents}) => recordEvents({type: eventTypeA, data: jsonBuffer(null)}),
      },
    })

    const isHandled = await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA()))

    expect(isHandled).to.be.true()
    expect(await this.query('SELECT * FROM recluse.stream')).to.have.rows([
      {name: id, type: typeA},
    ])
  })

  it('should immediately apply recorded events to the state', async function () {
    const createCommandA = () => ({type: commandTypeA})
    const id = 'aggregate-id-a'
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA],
        eventTypes: [eventTypeA],
        routeCommand: () => id,
        createInitialState: () => ({a: 0}),

        handleCommand: ({state, recordEvents}) => {
          recordEvents({type: eventTypeA, data: jsonBuffer({value: state.a + 111})})
          recordEvents(
            {type: eventTypeA, data: jsonBuffer({value: state.a + 111})},
            {type: eventTypeA, data: jsonBuffer({value: state.a + 111})}
          )
        },

        applyEvent: (state, {data}) => {
          const {value} = parseJsonBuffer(data)
          state.a = value
        },
      },
    })

    const isHandled = await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA()))
    const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, id))

    expect(isHandled).to.be.true()
    expect(events).to.have.length(3)
    expect(events[0].event).to.have.fields({data: jsonBuffer({value: 111})})
    expect(events[1].event).to.have.fields({data: jsonBuffer({value: 222})})
    expect(events[2].event).to.have.fields({data: jsonBuffer({value: 222})})
  })

  it('should not allow failures to affect the state for future calls', async function () {
    const createCommandA = isOkay => ({type: commandTypeA, data: {isOkay}})
    const id = 'aggregate-id-a'
    const notOkay = new Error('Not okay.')
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA],
        eventTypes: [eventTypeA],
        routeCommand: () => id,
        createInitialState: () => ({a: 0}),

        handleCommand: ({state, command: {data: {isOkay}}, recordEvents}) => {
          recordEvents({type: eventTypeA, data: jsonBuffer({value: state.a + 111})})

          if (!isOkay) throw notOkay
        },

        applyEvent: (state, {data}) => {
          const {value} = parseJsonBuffer(data)
          state.a = value
        },
      },
    })

    const isHandled = []
    let error
    isHandled.push(await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA(true))))
    try {
      await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA(false)))
    } catch (e) {
      error = e
    }
    isHandled.push(await this.inTransaction(async () => handleCommand(this.pgClient, createCommandA(true))))
    const [events] = await asyncIterableToArray(readEventsByStream(this.pgClient, id))

    expect(isHandled).to.deep.equal([true, true])
    expect(error).to.equal(notOkay)
    expect(events).to.have.length(2)
    expect(events[0].event).to.have.fields({data: jsonBuffer({value: 111})})
    expect(events[1].event).to.have.fields({data: jsonBuffer({value: 222})})
  })

  it('should be able to route to aggregate instances on a per-command basis', async function () {
    const createCommandA = (id, increment) => ({type: commandTypeA, data: {id, increment}})
    const idA = 'aggregate-id-a'
    const idB = 'aggregate-id-b'
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA],
        eventTypes: [eventTypeA],
        routeCommand: ({data: {id}}) => id,
        createInitialState: () => ({a: 0}),

        handleCommand: ({state: {a}, command: {data: {increment}}, recordEvents}) => {
          recordEvents({type: eventTypeA, data: jsonBuffer({current: a, increment})})
        },

        applyEvent: (state, {type, data}) => {
          const {increment} = parseJsonBuffer(data)
          state.a += increment
        },
      },
    })

    const isHandled = []
    await this.inTransaction(async () => {
      isHandled.push(await handleCommand(this.pgClient, createCommandA(idA, 111)))
      isHandled.push(await handleCommand(this.pgClient, createCommandA(idB, 222)))
      isHandled.push(await handleCommand(this.pgClient, createCommandA(idA, 333)))
      isHandled.push(await handleCommand(this.pgClient, createCommandA(idB, 444)))
    })

    const [eventsA] = await asyncIterableToArray(readEventsByStream(this.pgClient, idA))
    const [eventsB] = await asyncIterableToArray(readEventsByStream(this.pgClient, idB))

    expect(isHandled).to.deep.equal([true, true, true, true])
    expect(eventsA).to.have.length(2)
    expect(eventsA[0].event).to.have.fields({type: eventTypeA, data: jsonBuffer({current: 0, increment: 111})})
    expect(eventsA[1].event).to.have.fields({type: eventTypeA, data: jsonBuffer({current: 111, increment: 333})})
    expect(eventsB).to.have.length(2)
    expect(eventsB[0].event).to.have.fields({type: eventTypeA, data: jsonBuffer({current: 0, increment: 222})})
    expect(eventsB[1].event).to.have.fields({type: eventTypeA, data: jsonBuffer({current: 222, increment: 444})})
  })

  it('should support multiple aggregates', async function () {
    const createCommandA = () => ({type: commandTypeA})
    const createCommandB = () => ({type: commandTypeB})
    const idA = 'aggregate-id-a'
    const idB = 'aggregate-id-b'
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA],
        eventTypes: [eventTypeA],
        routeCommand: () => idA,
        createInitialState: () => ({count: 0}),
        handleCommand: ({state: {count}, recordEvents}) => recordEvents({type: eventTypeA, data: jsonBuffer(count)}),
        applyEvent: state => ++state.count,
      },

      [nameB]: {
        ...emptyAggregate,
        commandTypes: [commandTypeB],
        eventTypes: [eventTypeA],
        routeCommand: () => idB,
        createInitialState: () => ({count: 0}),
        handleCommand: ({state: {count}, recordEvents}) => recordEvents({type: eventTypeA, data: jsonBuffer(count)}),
        applyEvent: state => ++state.count,
      },
    })

    const isHandled = []
    await this.inTransaction(async () => {
      isHandled.push(await handleCommand(this.pgClient, createCommandA()))
      isHandled.push(await handleCommand(this.pgClient, createCommandB()))
      isHandled.push(await handleCommand(this.pgClient, createCommandA()))
      isHandled.push(await handleCommand(this.pgClient, createCommandB()))
    })

    const [eventsA] = await asyncIterableToArray(readEventsByStream(this.pgClient, idA))
    const [eventsB] = await asyncIterableToArray(readEventsByStream(this.pgClient, idB))

    expect(isHandled).to.deep.equal([true, true, true, true])
    expect(eventsA).to.have.length(2)
    expect(eventsA[0].event).to.have.fields({type: eventTypeA, data: jsonBuffer(0)})
    expect(eventsA[1].event).to.have.fields({type: eventTypeA, data: jsonBuffer(1)})
    expect(eventsB).to.have.length(2)
    expect(eventsB[0].event).to.have.fields({type: eventTypeA, data: jsonBuffer(0)})
    expect(eventsB[1].event).to.have.fields({type: eventTypeA, data: jsonBuffer(1)})
  })

  it('should throw an error when handling commands with unexpected types', async function () {
    const handleCommand = createCommandHandler({
      [nameA]: emptyAggregate,
    })

    const operation = this.inTransaction(async () => {
      await handleCommand(this.pgClient, {type: commandTypeA})
    })

    await expect(operation)
      .to.be.rejectedWith(`Unable to handle ${commandTypeA} command - no suitable aggregates found`)
  })

  it('should throw an error when handling commands that cannot be routed', async function () {
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA],
      },
    })

    const operation = this.inTransaction(async () => {
      await handleCommand(this.pgClient, {type: commandTypeA})
    })

    await expect(operation).to.be.rejectedWith(`Unable to handle ${commandTypeA} command - no suitable route found`)
  })

  it('should throw an error when recording events with unexpected types', async function () {
    const handleCommand = createCommandHandler({
      [nameA]: {
        ...emptyAggregate,
        commandTypes: [commandTypeA],
        routeCommand: () => 'aggregate-id-a',
        handleCommand: ({recordEvents}) => recordEvents({type: eventTypeA}),
      },
    })

    const operation = this.inTransaction(async () => {
      await handleCommand(this.pgClient, {type: commandTypeA})
    })

    await expect(operation).to.be.rejectedWith(`Aggregate ${nameA} cannot record ${eventTypeA} events`)
  })
}))
