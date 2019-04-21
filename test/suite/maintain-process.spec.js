const {createSandbox, match} = require('sinon')
const {expect} = require('chai')

const {asyncIterableToArray, consumeAsyncIterable, jsonBuffer, parseJsonBuffer, pgSpec} = require('../helper.js')
const {createClock} = require('../clock.js')

const {appendEvents} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')
const {maintainProcess} = require('../../src/process.js')
const {readCommands} = require('../../src/command.js')

describe('maintainProcess()', pgSpec(function () {
  const nameA = 'process-name-a'
  const idA = 'process-id-a'
  const streamTypeA = 'stream-type-a'
  const streamNameA = 'stream-name-a'
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const eventA = {type: eventTypeA, data: jsonBuffer(111)}
  const eventB = {type: eventTypeB, data: jsonBuffer(222)}
  const eventC = {type: eventTypeA, data: jsonBuffer(333)}
  const eventD = {type: eventTypeB, data: jsonBuffer(444)}

  const emptyProcess = {
    eventTypes: [],
    commandTypes: [],
    routeEvent: () => idA,
    createInitialState: () => jsonBuffer(null),
    handleEvent: () => {},
  }

  beforeEach(async function () {
    this.sandbox = createSandbox()

    await initializeSchema(this.pgClient)
  })

  afterEach(function () {
    this.sandbox.restore()
  })

  context('before iteration', function () {
    it('should support cancellation', async function () {
      const process = {
        ...emptyProcess,
      }
      this.sandbox.spy(process, 'routeEvent')
      await appendEvents(this.pgClient, streamTypeA, streamNameA, 0, [eventA])
      await maintainProcess(this.pgClient, nameA, emptyProcess).cancel()

      expect(process.routeEvent).to.not.have.been.called()
    })
  })

  context('while iterating', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, streamTypeA, streamNameA, 0, [eventA, eventB])
    })

    it('should process the events in the correct order', async function () {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        createInitialState: () => jsonBuffer(0),
        handleEvent: async ({event: {data}, executeCommands, replaceState, state}) => {
          const number = parseJsonBuffer(data)
          const total = parseJsonBuffer(state) + number

          executeCommands({type: commandTypeA, data: jsonBuffer({total, number})})
          replaceState(jsonBuffer(total))
        },
      }
      await consumeAsyncIterable(
        maintainProcess(this.pgPool, nameA, process),
        2,
        process => process.cancel(),
        wasProcessed => expect(wasProcessed).to.be.true()
      )
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(2)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: jsonBuffer({total: 111, number: 111})})
      expect(commands[1].command).to.have.fields({type: commandTypeA, data: jsonBuffer({total: 333, number: 222})})
    })

    it('should update the state only when replaceState() is called', async function () {
      await appendEvents(this.pgClient, streamTypeA, streamNameA, 2, [eventC])

      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        createInitialState: () => jsonBuffer(0),
        handleEvent: async ({event: {data, type}, executeCommands, replaceState, state}) => {
          const number = parseJsonBuffer(data)
          const total = parseJsonBuffer(state) + number

          executeCommands({type: commandTypeA, data: jsonBuffer({total, number})})

          if (type === eventTypeA) {
            replaceState(jsonBuffer(total))
          } else {
            jsonBuffer(total).copy(state)
          }
        },
      }
      await consumeAsyncIterable(
        maintainProcess(this.pgPool, nameA, process),
        3,
        process => process.cancel(),
        wasProcessed => expect(wasProcessed).to.be.true()
      )
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(3)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: jsonBuffer({total: 111, number: 111})})
      expect(commands[1].command).to.have.fields({type: commandTypeA, data: jsonBuffer({total: 333, number: 222})})
      expect(commands[2].command).to.have.fields({type: commandTypeA, data: jsonBuffer({total: 444, number: 333})})
    })

    it('should process different event types', async function () {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA, commandTypeB],
        handleEvent: async ({event: {type: eventType}, executeCommands}) => {
          const type = eventType === eventTypeA ? commandTypeA : commandTypeB

          executeCommands({type, data: jsonBuffer({eventType})})
        },
      }
      await consumeAsyncIterable(
        maintainProcess(this.pgPool, nameA, process),
        2,
        process => process.cancel(),
        wasProcessed => expect(wasProcessed).to.be.true()
      )
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(2)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: jsonBuffer({eventType: eventTypeA})})
      expect(commands[1].command).to.have.fields({type: commandTypeB, data: jsonBuffer({eventType: eventTypeB})})
    })

    it('should ignore event types that should not be processed', async function () {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {type}, executeCommands}) => {
          executeCommands({type: commandTypeA, data: jsonBuffer({type})})
        },
      }
      const expectedWasProcesed = [true, false]
      await consumeAsyncIterable(
        maintainProcess(this.pgPool, nameA, process),
        2,
        process => process.cancel(),
        (wasProcessed, i) => expect(wasProcessed).to.equal(expectedWasProcesed[i])
      )
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(1)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: jsonBuffer({type: eventTypeA})})
    })

    it('should ignore event types that do not route to a process ID', async function () {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        routeEvent: ({type}) => type === eventTypeA ? idA : null,
        handleEvent: async ({event: {type}, executeCommands}) => {
          executeCommands({type: commandTypeA, data: jsonBuffer({type})})
        },
      }
      const expectedWasProcesed = [true, false]
      await consumeAsyncIterable(
        maintainProcess(this.pgPool, nameA, process),
        2,
        process => process.cancel(),
        (wasProcessed, i) => expect(wasProcessed).to.equal(expectedWasProcesed[i])
      )
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(1)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: jsonBuffer({type: eventTypeA})})
    })

    it('should throw if an unexpected commnd is executed', async function () {
      const process = maintainProcess(this.pgPool, nameA, {
        ...emptyProcess,
        eventTypes: [eventTypeA],
        commandTypes: [commandTypeA],
        handleEvent: async ({executeCommands}) => {
          executeCommands({type: commandTypeB})
        },
      })

      await expect(consumeAsyncIterable(process, 1))
        .to.be.rejectedWith(`Process ${nameA} cannot execute ${commandTypeB} commands`)

      await process.cancel()
    })

    it('should handle errors while processing events', async function () {
      const releases = []
      const pool = {
        connect: async () => {
          const client = await this.createPgClient()
          this.sandbox.spy(client, 'release')
          releases.push(client.release)

          return client
        },
      }
      const error = new Error('You done goofed')
      const process = maintainProcess(pool, nameA, {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        handleEvent: async () => { throw error },
      })

      await expect(consumeAsyncIterable(process, 1)).to.be.rejectedWith(error)

      await process.cancel()
    })

    it('should be able to process new events when relying solely on notifications', async function () {
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
          maintainProcess(this.pgPool, nameA, process, {timeout: null}),
          4,
          process => process.cancel()
        ),

        appendEvents(this.pgClient, streamTypeA, streamNameA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(4)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).to.have.fields({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).to.have.fields({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).to.have.fields({type: commandTypeA, data: eventD.data})
    })

    it('should process new events when a notification is received before the timeout', async function () {
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
          maintainProcess(this.pgPool, nameA, process, {clock}),
          4,
          process => process.cancel()
        ),

        appendEvents(this.pgClient, streamTypeA, streamNameA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(4)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).to.have.fields({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).to.have.fields({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).to.have.fields({type: commandTypeA, data: eventD.data})
    })

    it('should process new events when the timeout fires before receiving a notification', async function () {
      const clock = createClock({immediate: true})

      const appendClient = await this.createPgClient()
      this.sandbox.stub(appendClient, 'query')
      appendClient.query.withArgs(match('NOTIFY')).callsFake(async () => {})
      appendClient.query.callThrough()

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
          maintainProcess(this.pgPool, nameA, process, {clock}),
          4,
          process => process.cancel()
        ),

        appendEvents(appendClient, streamTypeA, streamNameA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(4)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).to.have.fields({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).to.have.fields({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).to.have.fields({type: commandTypeA, data: eventD.data})
    })
  })

  context('when resuming the maintenance of an existing process', function () {
    beforeEach(async function () {
      this.process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }

      await appendEvents(this.pgClient, streamTypeA, streamNameA, 0, [eventA, eventB])
      await consumeAsyncIterable(
        maintainProcess(this.pgPool, nameA, this.process),
        2,
        process => process.cancel()
      )
    })

    it('should process new events in the correct order', async function () {
      await Promise.all([
        consumeAsyncIterable(
          maintainProcess(this.pgPool, nameA, this.process, {timeout: null}),
          2,
          process => process.cancel()
        ),

        appendEvents(this.pgClient, streamTypeA, streamNameA, 2, [eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(4)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).to.have.fields({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).to.have.fields({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).to.have.fields({type: commandTypeA, data: eventD.data})
    })
  })

  context('when multiple workers try to maintain the same process', function () {
    it('should cooperatively process events using a single worker at a time', async function () {
      const process = {
        ...emptyProcess,
        eventTypes: [eventTypeA, eventTypeB],
        commandTypes: [commandTypeA],
        handleEvent: async ({event: {data}, executeCommands}) => {
          executeCommands({type: commandTypeA, data})
        },
      }
      const maintain = () => consumeAsyncIterable(
        maintainProcess(this.pgPool, nameA, process, {timeout: null}),
        2,
        process => process.cancel()
      )

      await Promise.all([
        maintain(),
        maintain(),

        appendEvents(this.pgClient, streamTypeA, streamNameA, 0, [eventA, eventB, eventC, eventD]),
      ])
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(4)
      expect(commands[0].command).to.have.fields({type: commandTypeA, data: eventA.data})
      expect(commands[1].command).to.have.fields({type: commandTypeA, data: eventB.data})
      expect(commands[2].command).to.have.fields({type: commandTypeA, data: eventC.data})
      expect(commands[3].command).to.have.fields({type: commandTypeA, data: eventD.data})
    })
  })
}))
