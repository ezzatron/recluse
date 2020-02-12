const {appendEvents, readNextStreamOffset} = require('../../src/event.js')
const {createTestHelper} = require('../helper/pg.js')
const {initializeSchema} = require('../../src/schema.js')
const {serialization} = require('../../src/serialization/json.js')

describe('readNextStreamOffset()', () => {
  const pgHelper = createTestHelper()

  beforeEach(async () => {
    await initializeSchema(pgHelper.client)
  })

  const type = 'stream-type-a'
  const instance = 'stream-instance-a'
  const event = {type: 'event-type-a', data: 'a'}

  describe('with an empty stream', () => {
    it('should return offset 0', async () => {
      expect(await readNextStreamOffset(pgHelper.client, type, instance)).toBe(0)
    })

    it('should require a valid stream type', async () => {
      await expect(readNextStreamOffset(pgHelper.client)).rejects.toThrow('Invalid stream type')
    })

    it('should require a valid stream instance', async () => {
      await expect(readNextStreamOffset(pgHelper.client, type)).rejects.toThrow('Invalid stream instance')
    })
  })

  describe('with a non-empty stream', () => {
    beforeEach(async () => {
      await appendEvents(serialization, pgHelper.client, type, instance, 0, [event, event])
    })

    it('should return a positive offset', async () => {
      expect(await readNextStreamOffset(pgHelper.client, type, instance)).toBe(2)
    })
  })
})
