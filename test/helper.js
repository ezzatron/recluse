const {Client: PgClient} = require('pg')

module.exports = {
  pgSpec,
}

function pgSpec (spec) {
  return function () {
    before(async function () {
      this.pgInitClient = createPgClient('postgres')
      this.pgClient = createPgClient('recluse_test')

      await this.pgInitClient.connect()
      await this.pgInitClient.query('DROP DATABASE IF EXISTS recluse_test')
      await this.pgInitClient.query('CREATE DATABASE recluse_test')
      await this.pgClient.connect()
    })

    afterEach(async function () {
      await this.pgClient.query('DROP SCHEMA recluse CASCADE')
    })

    after(async function () {
      try {
        await this.pgClient.end()
      } catch (error) {}

      await this.pgInitClient.query('DROP DATABASE recluse_test')

      try {
        await this.pgInitClient.end()
      } catch (error) {}
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
