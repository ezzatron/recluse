const {createTestHelper, TIME_PATTERN} = require('../helper/pg.js')
const {asyncIterableToArray, consumeAsyncIterable} = require('../helper/async.js')
const {executeCommands, readCommands} = require('../../src/command.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')

describe('readCommands()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    await initializeSchema(pgHelper.client)
  })

  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}

  describe('with no commands', () => {
    it('should return an empty result for ID 0', async () => {
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(0)
    })

    it('should return an empty result for positive IDs', async () => {
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client, 111))

      expect(commands).toHaveLength(0)
    })

    it('should support cancellation', async () => {
      expect(await readCommands(serialization, pgHelper.client).cancel()).toBeUndefined()
    })
  })

  describe('with existing commands', () => {
    beforeEach(async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB])
    })

    it('should return the correct commands for ID 0', async () => {
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(2)
      expect(commands[0]).toMatchObject({id: 0, source: sourceA, executedAt: TIME_PATTERN, handledAt: null})
      expect(commands[0].command).toEqual(commandA)
      expect(commands[1]).toMatchObject({id: 1, source: sourceA, executedAt: TIME_PATTERN, handledAt: null})
      expect(commands[1].command).toEqual(commandB)
    })

    it('should return the correct commands for positive IDs that exist', async () => {
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client, 1))

      expect(commands).toHaveLength(1)
      expect(commands[0]).toMatchObject({id: 1, source: sourceA, executedAt: TIME_PATTERN, handledAt: null})
      expect(commands[0].command).toEqual(commandB)
    })

    it('should return an empty result for positive IDs that do not exist', async () => {
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client, 111))

      expect(commands).toHaveLength(0)
    })

    it('should support cancellation', async () => {
      await consumeAsyncIterable(
        readCommands(serialization, pgHelper.client),
        1,
        commands => commands.cancel(),
        command => expect(command).toBeDefined(),
      )
    })
  })
})
