const {createCommandHandler, maintainCommandHandler} = require('../../src/command-handler.js')
const {executeCommands} = require('../../src/command.js')
const {readEvents} = require('../../src/event.js')
const {UNIQUE_VIOLATION} = require('../../src/pg.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')
const {asyncIterableToArray, consumeAsyncIterable} = require('../helper/async.js')
const {createClock} = require('../helper/clock.js')
const {createTestHelper} = require('../helper/pg.js')

describe('maintainCommandHandler()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    await initializeSchema(pgHelper.client)
  })

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

  let handleCommand

  beforeEach(async () => {
    handleCommand = createCommandHandler(
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
  })

  describe('before iteration', () => {
    it('should support cancellation', async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB])
      await maintainCommandHandler(serialization, pgHelper.client, handleCommand).cancel()
      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(0)
    })
  })

  describe('while iterating', () => {
    beforeEach(async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB])
    })

    it('should handle commands', async () => {
      await consumeAsyncIterable(
        maintainCommandHandler(serialization, pgHelper.pool, handleCommand),
        2,
        commands => commands.cancel(),
      )
      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(2)
      expect(events[0].event).toEqual({type: eventTypeA, data: commandA.data})
      expect(events[1].event).toEqual({type: eventTypeB, data: commandB.data})
    })

    it('should throw if commands cannot be handled', async () => {
      const error = new Error('Unique violation')
      error.code = UNIQUE_VIOLATION

      const releases = []
      const pool = {
        connect: async () => {
          const client = await pgHelper.createClient()

          const clientQuery = client.query.bind(client)
          jest.spyOn(client, 'query').mockImplementation((text, ...args) => {
            if (typeof text === 'string' && text.startsWith('INSERT INTO recluse.stream')) return Promise.reject(error)

            return clientQuery(text, ...args)
          })

          jest.spyOn(client, 'release')
          releases.push(client.release)

          return client
        },
      }

      const commands = maintainCommandHandler(serialization, pool, handleCommand)

      await expect(consumeAsyncIterable(commands, 1)).rejects.toThrow('Unable to handle command-type-a command')
      expect(releases[1]).toHaveBeenCalled()

      await commands.cancel()
    })

    it('should handle new commands when relying solely on notifications', async () => {
      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(serialization, pgHelper.pool, handleCommand, {timeout: null}),
          4,
          commands => commands.cancel(),
        ),

        executeCommands(serialization, pgHelper.client, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(4)
      expect(events[0].event).toEqual({type: eventTypeA, data: commandA.data})
      expect(events[1].event).toEqual({type: eventTypeB, data: commandB.data})
      expect(events[2].event).toEqual({type: eventTypeA, data: commandC.data})
      expect(events[3].event).toEqual({type: eventTypeB, data: commandD.data})
    })

    it('should handle new commands when a notification is received before the timeout', async () => {
      const clock = createClock()

      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(serialization, pgHelper.pool, handleCommand, {clock}),
          4,
          commands => commands.cancel(),
        ),

        executeCommands(serialization, pgHelper.client, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(4)
      expect(events[0].event).toEqual({type: eventTypeA, data: commandA.data})
      expect(events[1].event).toEqual({type: eventTypeB, data: commandB.data})
      expect(events[2].event).toEqual({type: eventTypeA, data: commandC.data})
      expect(events[3].event).toEqual({type: eventTypeB, data: commandD.data})
    })

    it('should handle new commands when the timeout fires before receiving a notification', async () => {
      const clock = createClock({immediate: true})

      const executeClient = await pgHelper.createClient()
      const executeClientQuery = executeClient.query.bind(executeClient)
      jest.spyOn(executeClient, 'query').mockImplementation((text, ...args) => {
        if (typeof text === 'string' && text.startsWith('NOTIFY')) return Promise.resolve()

        return executeClientQuery(text, ...args)
      })

      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(serialization, pgHelper.pool, handleCommand, {clock}),
          4,
          commands => commands.cancel(),
        ),

        executeCommands(serialization, executeClient, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(4)
      expect(events[0].event).toEqual({type: eventTypeA, data: commandA.data})
      expect(events[1].event).toEqual({type: eventTypeB, data: commandB.data})
      expect(events[2].event).toEqual({type: eventTypeA, data: commandC.data})
      expect(events[3].event).toEqual({type: eventTypeB, data: commandD.data})
    })
  })

  describe('when resuming handling commands after previous handling', () => {
    beforeEach(async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB])
      await consumeAsyncIterable(
        maintainCommandHandler(serialization, pgHelper.pool, handleCommand),
        2,
        commands => commands.cancel(),
      )
    })

    it('should handle only unhandled commands', async () => {
      await Promise.all([
        consumeAsyncIterable(
          maintainCommandHandler(serialization, pgHelper.pool, handleCommand, {timeout: null}),
          2,
          commands => commands.cancel(),
        ),

        executeCommands(serialization, pgHelper.client, sourceA, [commandC, commandD]),
      ])
      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(4)
      expect(events[0].event).toEqual({type: eventTypeA, data: commandA.data})
      expect(events[1].event).toEqual({type: eventTypeB, data: commandB.data})
      expect(events[2].event).toEqual({type: eventTypeA, data: commandC.data})
      expect(events[3].event).toEqual({type: eventTypeB, data: commandD.data})
    })
  })

  describe('when multiple workers try handle commands', () => {
    it('should cooperatively handle commands using a single worker at a time', async () => {
      const maintain = () => consumeAsyncIterable(
        maintainCommandHandler(serialization, pgHelper.pool, handleCommand, {timeout: null}),
        2,
        commands => commands.cancel(),
      )

      await Promise.all([
        maintain(),
        maintain(),

        executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB, commandC, commandD]),
      ])

      const [events] = await asyncIterableToArray(readEvents(serialization, pgHelper.client))

      expect(events).toHaveLength(4)
      expect(events[0].event).toEqual({type: eventTypeA, data: commandA.data})
      expect(events[1].event).toEqual({type: eventTypeB, data: commandB.data})
      expect(events[2].event).toEqual({type: eventTypeA, data: commandC.data})
      expect(events[3].event).toEqual({type: eventTypeB, data: commandD.data})
    })
  })
})
