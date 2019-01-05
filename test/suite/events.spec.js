const {Client} = require('pg')
const {expect} = require('chai')

const {initializeSchema} = require('../../src/index.js')

describe('Events', function () {
  before(async function () {
    const {PGHOST, PGPORT, PGUSER, PGPASSWORD} = process.env
    const host = PGHOST || 'localhost'
    const port = parseInt(PGPORT || '5432')
    const user = PGUSER || 'postgres'
    const password = PGPASSWORD || ''

    this.pgInitClient = new Client({host, port, user, password, database: 'postgres'})
    await this.pgInitClient.connect()
    await this.pgInitClient.query('DROP DATABASE IF EXISTS recluse_test')
    await this.pgInitClient.query('CREATE DATABASE recluse_test')

    this.pgClient = new Client({host, port, user, password, database: 'recluse_test'})
    await this.pgClient.connect()
  })

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

  after(async function () {
    this.pgClient && await this.pgClient.end()
    await this.pgInitClient.query('DROP DATABASE recluse_test')
    await this.pgInitClient.end()
  })
})
