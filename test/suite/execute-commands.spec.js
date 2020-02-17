const {COMMAND: CHANNEL} = require('../../src/channel.js')
const {executeCommands, readCommands} = require('../../src/command.js')
const {waitForNotification} = require('../../src/pg.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')
const {asyncIterableToArray} = require('../helper/async.js')
const {createTestHelper} = require('../helper/pg.js')

describe('executeCommands()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    pgHelper.trackSchemas('recluse')
    await initializeSchema(pgHelper.client)
  })

  const sourceA = 'command-source-a'
  const commandTypeA = 'command-type-a'
  const commandTypeB = 'command-type-b'
  const commandA = {type: commandTypeA, data: 'a'}
  const commandB = {type: commandTypeB, data: 'b'}
  const commandC = {type: commandTypeA, data: 'c'}
  const commandD = {type: commandTypeB, data: 'd'}

  describe('with no commands', () => {
    it('should be able to record commands', async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(2)
      expect(commands[0].command).toEqual(commandA)
      expect(commands[1].command).toEqual(commandB)
    })

    it('should be able to record commands with null data', async () => {
      const command = {type: commandTypeA, data: null}
      await executeCommands(serialization, pgHelper.client, sourceA, [command])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(1)
      expect(commands[0].command).toEqual({type: commandTypeA, data: null})
    })

    it('should be able to record commands with undefined data', async () => {
      const command = {type: commandTypeA}
      await executeCommands(serialization, pgHelper.client, sourceA, [command])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client))

      expect(commands).toHaveLength(1)
      expect(commands[0].command).toEqual({type: commandTypeA, data: undefined})
    })
  })

  describe('with existing commands', () => {
    beforeEach(async () => {
      await pgHelper.inTransaction(async () => executeCommands(serialization, pgHelper.client, sourceA, [commandA, commandB]))
    })

    it('should be able to record commands', async () => {
      await executeCommands(serialization, pgHelper.client, sourceA, [commandC, commandD])
      const [commands] = await asyncIterableToArray(readCommands(serialization, pgHelper.client, 2))

      expect(commands).toHaveLength(2)
      expect(commands[0].command).toEqual(commandC)
      expect(commands[1].command).toEqual(commandD)
    })
  })

  describe('with other clients listening for commands', () => {
    let secondaryPgClient, waitForCommand

    beforeEach(async () => {
      secondaryPgClient = await pgHelper.createClient()
      await secondaryPgClient.query(`LISTEN ${CHANNEL}`)
      waitForCommand = waitForNotification(secondaryPgClient, CHANNEL)
    })

    it('should notify listening clients when recording commands', async () => {
      const [notification] = await Promise.all([
        waitForCommand,
        pgHelper.inTransaction(async () => executeCommands(serialization, pgHelper.client, sourceA, [commandA])),
      ])

      expect(notification.channel).toBe(CHANNEL)
    })
  })
})
