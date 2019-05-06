const {expect} = require('chai')

const {consumeAsyncIterable, pgSpec, TIME_PATTERN} = require('../helper.js')

const {executeCommands, readUnhandledCommandsContinuously} = require('../../src/command.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')

describe('readUnhandledCommandsContinuously()', pgSpec(function () {
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with no commands', function () {
    it('should support cancellation', async function () {
      await readUnhandledCommandsContinuously(serialization, this.pgClient).cancel()
    })
  })

  context('with only unhandled commands', function () {
    beforeEach(async function () {
      await executeCommands(serialization, this.pgClient, sourceA, [commandA, commandB])
    })

    it('should return all commands', async function () {
      const expectedWrappers = [
        {id: 0, source: sourceA, executedAt: TIME_PATTERN, handledAt: null},
        {id: 1, source: sourceA, executedAt: TIME_PATTERN, handledAt: null},
      ]
      const expected = [
        commandA,
        commandB,
      ]

      await consumeAsyncIterable(
        readUnhandledCommandsContinuously(serialization, this.pgClient),
        expected.length,
        commands => commands.cancel(),
        wrapper => {
          expect(wrapper).to.have.fields(expectedWrappers.shift())
          expect(wrapper.command).to.deep.equal(expected.shift())
        }
      )
    })
  })

  context('with some handled commands', function () {
    beforeEach(async function () {
      await executeCommands(serialization, this.pgClient, sourceA, [commandA, commandB])
      await this.query('UPDATE recluse.command SET handled_at = now() WHERE id = 0')
    })

    it('should return only the unhandled commands', async function () {
      const expectedWrappers = [
        {id: 1, source: sourceA, executedAt: TIME_PATTERN, handledAt: null},
      ]
      const expected = [
        commandB,
      ]

      await consumeAsyncIterable(
        readUnhandledCommandsContinuously(serialization, this.pgClient, {id: 1}),
        expected.length,
        commands => commands.cancel(),
        wrapper => {
          expect(wrapper).to.have.fields(expectedWrappers.shift())
          expect(wrapper.command).to.deep.equal(expected.shift())
        }
      )
    })
  })
}))
