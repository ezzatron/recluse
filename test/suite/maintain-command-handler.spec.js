const {createSandbox, match} = require('sinon')
const {expect} = require('chai')

const {asyncIterableToArray, consumeAsyncIterable, pgSpec} = require('../helper.js')
const {createClock} = require('../clock.js')

const {createCommandHandler, executeCommands, maintainCommandHandler} = require('../../src/command.js')
const {initializeSchema} = require('../../src/schema.js')
const {readEvents} = require('../../src/event.js')
const {UNIQUE_VIOLATION} = require('../../src/pg.js')

describe('maintainCommandHandler()', pgSpec(function () {
  const nameA = 'aggregate-name-a'
  const typeA = 'aggregate-type-a'
  const idA = 'aggregate-id-a'
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: Buffer.from('a')}
  const commandB = {type: commandTypeB, data: Buffer.from('b')}
  const commandC = {type: commandTypeA, data: Buffer.from('c')}
  const commandD = {type: commandTypeB, data: Buffer.from('d')}
  const eventTypeA = 'event-type-a'
  const eventTypeB = 'event-type-b'

  beforeEach(async function () {
    this.sandbox = createSandbox()

    await initializeSchema(this.pgClient)

    this.handleCommand = createCommandHandler({
      [nameA]: {
        type: typeA,
        commandTypes: [commandTypeA, commandTypeB],
        eventTypes: [eventTypeA, eventTypeB],
        routeCommand: () => idA,
        createInitialState: () => {},
        handleCommand: ({command: {type, data}, recordEvents}) => {
          switch (type) {
            case commandTypeA: return recordEvents({type: eventTypeA, data})
            case commandTypeB: return recordEvents({type: eventTypeB, data})
          }
        },
        applyEvent: () => {},
      },
    })
  })

  afterEach(function () {
    this.sandbox.restore()
  })

  context('before iteration', function () {
    it('should support cancellation', async function () {
      await executeCommands(this.pgClient, sourceA, [commandA, commandB])
      await maintainCommandHandler(this.pgClient, this.handleCommand).cancel()
      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(0)
    })
  })

  context('while iterating', function () {
    beforeEach(async function () {
      await executeCommands(this.pgClient, sourceA, [commandA, commandB])
    })

    it('should handle commands', async function () {
      await consumeAsyncIterable(
        maintainCommandHandler(this.pgPool, this.handleCommand),
        2,
        commands => commands.cancel()
      )
      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(2)
      expect(events[0].event).to.have.fields({type: eventTypeA, data: commandA.data})
      expect(events[1].event).to.have.fields({type: eventTypeB, data: commandB.data})
    })

    it('should throw if commands cannot be handled', async function () {
      const error = new Error('Unique violation')
      error.code = UNIQUE_VIOLATION

      const releases = []
      const pool = {
        connect: async () => {
          const client = await this.createPgClient()
          this.sandbox.stub(client, 'query')
          client.query.withArgs(match('INSERT INTO recluse.stream')).callsFake(async () => { throw error })
          client.query.callThrough()
          this.sandbox.spy(client, 'release')

          releases.push(client.release)

          return client
        },
      }

      const commands = maintainCommandHandler(pool, this.handleCommand)

      await expect(consumeAsyncIterable(commands, 1)).to.be.rejectedWith('Unable to handle command-type-a command')
      expect(releases[1]).to.have.been.called()

      await commands.cancel()
    })

    it('should handle new commands when relying solely on notifications', async function () {
      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(this.pgPool, this.handleCommand, {timeout: null}),
          4,
          commands => commands.cancel()
        ),

        executeCommands(this.pgClient, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(4)
      expect(events[0].event).to.have.fields({type: eventTypeA, data: commandA.data})
      expect(events[1].event).to.have.fields({type: eventTypeB, data: commandB.data})
      expect(events[2].event).to.have.fields({type: eventTypeA, data: commandC.data})
      expect(events[3].event).to.have.fields({type: eventTypeB, data: commandD.data})
    })

    it('should handle new commands when a notification is received before the timeout', async function () {
      const clock = createClock()

      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(this.pgPool, this.handleCommand, {clock}),
          4,
          commands => commands.cancel()
        ),

        executeCommands(this.pgClient, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(4)
      expect(events[0].event).to.have.fields({type: eventTypeA, data: commandA.data})
      expect(events[1].event).to.have.fields({type: eventTypeB, data: commandB.data})
      expect(events[2].event).to.have.fields({type: eventTypeA, data: commandC.data})
      expect(events[3].event).to.have.fields({type: eventTypeB, data: commandD.data})
    })

    it('should handle new commands when the timeout fires before receiving a notification', async function () {
      const clock = createClock({immediate: true})

      const executeClient = await this.createPgClient()
      this.sandbox.stub(executeClient, 'query')
      executeClient.query.withArgs(match('NOTIFY')).callsFake(async () => {})
      executeClient.query.callThrough()

      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(this.pgPool, this.handleCommand, {clock}),
          4,
          commands => commands.cancel()
        ),

        executeCommands(executeClient, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(4)
      expect(events[0].event).to.have.fields({type: eventTypeA, data: commandA.data})
      expect(events[1].event).to.have.fields({type: eventTypeB, data: commandB.data})
      expect(events[2].event).to.have.fields({type: eventTypeA, data: commandC.data})
      expect(events[3].event).to.have.fields({type: eventTypeB, data: commandD.data})
    })
  })

  context('when resuming handling commands after previous handling', function () {
    beforeEach(async function () {
      await executeCommands(this.pgClient, sourceA, [commandA, commandB])
      await consumeAsyncIterable(
        maintainCommandHandler(this.pgPool, this.handleCommand),
        2,
        commands => commands.cancel()
      )
    })

    it('should handle only unhandled commands', async function () {
      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(this.pgPool, this.handleCommand, {timeout: null}),
          2,
          commands => commands.cancel()
        ),

        executeCommands(this.pgClient, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(4)
      expect(events[0].event).to.have.fields({type: eventTypeA, data: commandA.data})
      expect(events[1].event).to.have.fields({type: eventTypeB, data: commandB.data})
      expect(events[2].event).to.have.fields({type: eventTypeA, data: commandC.data})
      expect(events[3].event).to.have.fields({type: eventTypeB, data: commandD.data})
    })
  })

  context('when multiple workers try handle commands', function () {
    it('should cooperatively handle commands using a single worker at a time', async function () {
      const maintain = () => consumeAsyncIterable(
        maintainCommandHandler(this.pgPool, this.handleCommand, {timeout: null}),
        2,
        commands => commands.cancel()
      )

      await Promise.all([
        maintain(),
        maintain(),

        executeCommands(this.pgClient, sourceA, [commandA, commandB, commandC, commandD]),
      ])

      const [events] = await asyncIterableToArray(readEvents(this.pgClient))

      expect(events).to.have.length(4)
      expect(events[0].event).to.have.fields({type: eventTypeA, data: commandA.data})
      expect(events[1].event).to.have.fields({type: eventTypeB, data: commandB.data})
      expect(events[2].event).to.have.fields({type: eventTypeA, data: commandC.data})
      expect(events[3].event).to.have.fields({type: eventTypeB, data: commandD.data})
    })
  })
}))
