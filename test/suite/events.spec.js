const {expect} = require('chai')
const {pgSpec} = require('../helper.js')

const {initializeSchema} = require('../../src/index.js')

describe('Events', pgSpec(function () {
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

    it('should be safe to run multiple times', async function () {
      await initializeSchema(this.pgClient)
      await initializeSchema(this.pgClient)
      const actual = await this.pgClient.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
      )

      expect(actual.rows).to.deep.include({table_name: 'global_offset'})
      expect(actual.rows).to.deep.include({table_name: 'stream'})
      expect(actual.rows).to.deep.include({table_name: 'event'})
    })
  })
}))
