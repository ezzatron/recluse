const {expect} = require('chai')

const {asyncIterableToArray, pgSpec} = require('../helper.js')

const {COMMAND: CHANNEL} = require('../../src/channel.js')
const {executeCommands, readCommands} = require('../../src/command.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')
const {waitForNotification} = require('../../src/pg.js')

describe('executeCommands()', pgSpec(function () {
  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}
  const commandC = {type: commandTypeA, data: 'c'}
  const commandD = {type: commandTypeB, data: 'd'}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with no commands', function () {
    it('should be able to record commands', async function () {
      await executeCommands(serialization, this.pgClient, sourceA, [commandA, commandB])
      const [commands] = await asyncIterableToArray(readCommands(serialization, this.pgClient))

      expect(commands).to.have.length(2)
      expect(commands[0].command).to.deep.equal(commandA)
      expect(commands[1].command).to.deep.equal(commandB)
    })

    it('should be able to record commands with null data', async function () {
      const command = {type: commandTypeA, data: null}
      await executeCommands(serialization, this.pgClient, sourceA, [command])
      const [commands] = await asyncIterableToArray(readCommands(serialization, this.pgClient))

      expect(commands).to.have.length(1)
      expect(commands[0].command).to.deep.equal({type: commandTypeA})
      expect(commands[0].command.data).to.be.undefined()
    })

    it('should be able to record commands with undefined data', async function () {
      const command = {type: commandTypeA}
      await executeCommands(serialization, this.pgClient, sourceA, [command])
      const [commands] = await asyncIterableToArray(readCommands(serialization, this.pgClient))

      expect(commands).to.have.length(1)
      expect(commands[0].command).to.deep.equal({type: commandTypeA})
      expect(commands[0].command.data).to.be.undefined()
    })
  })

  context('with existing commands', function () {
    beforeEach(async function () {
      await this.inTransaction(async () => executeCommands(serialization, this.pgClient, sourceA, [commandA, commandB]))
    })

    it('should be able to record commands', async function () {
      await executeCommands(serialization, this.pgClient, sourceA, [commandC, commandD])
      const [commands] = await asyncIterableToArray(readCommands(serialization, this.pgClient, 2))

      expect(commands).to.have.length(2)
      expect(commands[0].command).to.deep.equal(commandC)
      expect(commands[1].command).to.deep.equal(commandD)
    })
  })

  context('with other clients listening for commands', function () {
    beforeEach(async function () {
      this.secondaryPgClient = await this.createPgClient()
      await this.secondaryPgClient.query(`LISTEN ${CHANNEL}`)
      this.waitForCommand = waitForNotification(this.secondaryPgClient, CHANNEL)
    })

    it('should notify listening clients when recording commands', async function () {
      const [notification] = await Promise.all([
        this.waitForCommand,
        this.inTransaction(async () => executeCommands(serialization, this.pgClient, sourceA, [commandA])),
      ])

      expect(notification.channel).to.equal(CHANNEL)
    })
  })
}))
