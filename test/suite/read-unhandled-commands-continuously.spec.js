const {consumeAsyncIterable} = require('../helper/async.js')
const {createTestHelper, TIME_PATTERN} = require('../helper/pg.js')
const {executeCommands, readUnhandledCommandsContinuously} = require('../../src/command.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')

describe('readUnhandledCommandsContinuously()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    await initializeSchema(pgHelper.client)
  })

  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}

  beforeEach(async () => {
    await initializeSchema(pgHelper.client)
  })

  describe('with no commands', () => {
    it('should support cancellation', async () => {
      expect(await readUnhandledCommandsContinuously(serialization, pgHelper.client).cancel()).toBeUndefined()
    })
  })

  describe('with only unhandled commands', () => {
    beforeEach(async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB])
    })

    it('should return all commands', async () => {
      const expectedWrappers = [
        {id: 0, source: sourceA, executedAt: TIME_PATTERN, handledAt: null},
        {id: 1, source: sourceA, executedAt: TIME_PATTERN, handledAt: null},
      ]
      const expected = [
        commandA,
        commandB,
      ]

      await consumeAsyncIterable(
        readUnhandledCommandsContinuously(serialization, pgHelper.client),
        expected.length,
        commands => commands.cancel(),
        wrapper => {
          expect(wrapper).toMatchObject(expectedWrappers.shift())
          expect(wrapper.command).toEqual(expected.shift())
        },
      )
    })
  })

  describe('with some handled commands', () => {
    beforeEach(async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB])
      await pgHelper.query('UPDATE recluse.command SET handled_at = now() WHERE id = 0')
    })

    it('should return only the unhandled commands', async () => {
      const expectedWrappers = [
        {id: 1, source: sourceA, executedAt: TIME_PATTERN, handledAt: null},
      ]
      const expected = [
        commandB,
      ]

      await consumeAsyncIterable(
        readUnhandledCommandsContinuously(serialization, pgHelper.client, {id: 1}),
        expected.length,
        commands => commands.cancel(),
        wrapper => {
          expect(wrapper).toMatchObject(expectedWrappers.shift())
          expect(wrapper.command).toEqual(expected.shift())
        },
      )
    })
  })
})
