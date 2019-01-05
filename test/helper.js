const {Client: PgClient} = require('pg')

module.exports = {
  pgSpec,
}

function pgSpec (spec) {
  return function () {
    before(async function () {
      this.pgInitClient = createPgClient('postgres')
      await this.pgInitClient.connect()
      await this.pgInitClient.query('DROP DATABASE IF EXISTS recluse_test')
    })

    beforeEach(async function () {
      await this.pgInitClient.query('CREATE DATABASE recluse_test')

      this.pgClient = createPgClient('recluse_test')
      await this.pgClient.connect()
    })

    afterEach(async function () {
      this.pgClient && await this.pgClient.end()
      await this.pgInitClient.query('DROP DATABASE recluse_test')
    })

    after(async function () {
      await this.pgInitClient.end()
    })

    spec.call(this)
  }
}

function createPgClient (database) {
  const {PGHOST, PGPORT, PGUSER, PGPASSWORD} = process.env

  return new PgClient({
    host: PGHOST || 'localhost',
    port: parseInt(PGPORT || '5432'),
    user: PGUSER || 'postgres',
    password: PGPASSWORD || '',
    database,
  })
}
