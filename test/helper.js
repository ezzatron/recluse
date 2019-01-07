const {Client: PgClient} = require('pg')

module.exports = {
  pgSpec,
}

function pgSpec (spec) {
  return function () {
    before(async function () {
      this.pgInitClient = createPgClient('postgres')
      this.pgClient = createPgClient('recluse_test')
      this.query = this.pgClient.query.bind(this.pgClient)
      this.inTransaction = async (fn, pgClient = this.pgClient) => inTransaction(pgClient, fn)

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

async function inTransaction (pgClient, fn) {
  let result

  await pgClient.query('BEGIN')

  try {
    result = await fn()
  } catch (error) {
    await pgClient.query('ROLLBACK')

    throw error
  }

  await pgClient.query('COMMIT')

  return result
}
