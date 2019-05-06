const {expect} = require('chai')

const {pgSpec} = require('../helper.js')

const {appendEvents, readNextStreamOffset} = require('../../src/event.js')
const {initializeSchema} = require('../../src/schema.js')

describe('readNextStreamOffset()', pgSpec(function () {
  const type = 'stream-type-a'
  const instance = 'stream-instance-a'
  const event = {type: 'event-type-a', data: Buffer.from('a')}

  beforeEach(async function () {
    await initializeSchema(this.pgClient)
  })

  context('with an empty stream', function () {
    it('should return offset 0', async function () {
      expect(await readNextStreamOffset(this.pgClient, type, instance)).to.equal(0)
    })

    it('should require a valid stream type', async function () {
      await expect(readNextStreamOffset(this.pgClient)).to.be.rejectedWith('Invalid stream type')
    })

    it('should require a valid stream instance', async function () {
      await expect(readNextStreamOffset(this.pgClient, type)).to.be.rejectedWith('Invalid stream instance')
    })
  })

  context('with a non-empty stream', function () {
    beforeEach(async function () {
      await appendEvents(this.pgClient, type, instance, 0, [event, event])
    })

    it('should return a positive offset', async function () {
      expect(await readNextStreamOffset(this.pgClient, type, instance)).to.equal(2)
    })
  })
}))
