const {createPgClient, destroyPgClient} = require('../helper.js')
const {expect} = require('chai')

const {initializeSchema} = require('../../src/index.js')

describe('Events', function () {
  before(createPgClient)

  describe('initializeSchema()', function () {
    it('should create the necessary tables', async function () {
      await initializeSchema(this.pgClient)
      const actual = await this.pgClient.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
      )

      expect(actual.rows).to.deep.include({table_name: 'global_offset'})
      expect(actual.rows).to.deep.include({table_name: 'stream'})
      expect(actual.rows).to.deep.include({table_name: 'event'})
    })
  })

  after(destroyPgClient)
})
