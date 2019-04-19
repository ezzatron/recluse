const {expect} = require('chai')

const {asyncIterableToArray, consumeAsyncIterable, pgSpec, TIME_PATTERN} = require('../helper.js')

const {executeCommands, readCommands} = require('../../src/command.js')
const {initializeSchema} = require('../../src/schema.js')

describe('readCommands()', pgSpec(function () {
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandDataA = Buffer.from('a')
  const commandDataB = Buffer.from('b')
  const commandA = {type: commandTypeA, data: commandDataA}
  const commandB = {type: commandTypeB, data: commandDataB}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with no commands', function () {
    it('should return an empty result for ID 0', async function () {
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(0)
    })

    it('should return an empty result for positive IDs', async function () {
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient, 111))

      expect(commands).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await readCommands(this.pgClient).cancel()
    })
  })

  context('with existing commands', function () {
    beforeEach(async function () {
      await executeCommands(this.pgClient, sourceA, [commandA, commandB])
    })

    it('should return the correct commands for ID 0', async function () {
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient))

      expect(commands).to.have.length(2)
      expect(commands[0]).to.have.fields({
        id: 0,
        source: sourceA,
        executedAt: TIME_PATTERN,
        handledAt: null,
      })
      expect(commands[0].command).to.have.fields({
        type: commandTypeA,
        data: commandDataA,
      })
      expect(commands[1]).to.have.fields({
        id: 1,
        source: sourceA,
        executedAt: TIME_PATTERN,
        handledAt: null,
      })
      expect(commands[1].command).to.have.fields({
        type: commandTypeB,
        data: commandDataB,
      })
    })

    it('should return the correct commands for positive IDs that exist', async function () {
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient, 1))

      expect(commands).to.have.length(1)
      expect(commands[0]).to.have.fields({
        id: 1,
        source: sourceA,
        executedAt: TIME_PATTERN,
        handledAt: null,
      })
      expect(commands[0].command).to.have.fields({
        type: commandTypeB,
        data: commandDataB,
      })
    })

    it('should return an empty result for positive IDs that do not exist', async function () {
      const [commands] = await asyncIterableToArray(readCommands(this.pgClient, 111))

      expect(commands).to.have.length(0)
    })

    it('should support cancellation', async function () {
      await consumeAsyncIterable(
        readCommands(this.pgClient),
        1,
        commands => commands.cancel(),
        command => expect(command).to.exist()
      )
    })
  })
}))
