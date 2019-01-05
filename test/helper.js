const {Client} = require('pg')

module.exports = {
  createPgClient,
  destroyPgClient,
}

async function createPgClient () {
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
}

async function destroyPgClient () {
  this.pgClient && await this.pgClient.end()
  await this.pgInitClient.query('DROP DATABASE recluse_test')
  await this.pgInitClient.end()
}
