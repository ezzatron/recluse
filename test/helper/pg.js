const {Client, Pool} = require('pg')

const {inTransaction} = require('../../src/pg.js')

module.exports = {
  createTestHelper,

  TIME_PATTERN: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+\+00$/,
}

function createTestHelper () {
  const {PGHOST, PGPORT, PGUSER, PGPASSWORD} = process.env
  const connectOptions = {
    host: PGHOST || 'localhost',
    port: parseInt(PGPORT || '5432'),
    user: PGUSER || 'postgres',
    password: PGPASSWORD || '',
  }

  const database = `recluse_test_${Math.random().toString(36).substring(2, 15)}`
  const clients = []
  let client, initClient, pool, query

  beforeAll(async function pgTestHelperBeforeAll () {
    initClient = new Client({...connectOptions, database: 'postgres'})

    await initClient.connect()
    await initClient.query(`DROP DATABASE IF EXISTS ${database}`)
    await initClient.query(`CREATE DATABASE ${database}`)

    pool = new Pool({...connectOptions, database})
  })

  beforeEach(async function pgTestHelperBeforeEach () {
    client = await createClient()
    query = client.query.bind(client)
  })

  afterEach(async function pgTestHelperAfterEach () {
    await client.query('DROP SCHEMA recluse CASCADE')

    for (const client of clients) {
      try {
        client.release()
      } catch (error) {}
    }
  })

  afterAll(async function pgTestHelperAfterAll () {
    try {
      await pool.end()
    } catch (error) {}

    await initClient.query(`DROP DATABASE ${database}`)

    try {
      await initClient.end()
    } catch (error) {}
  })

  async function createClient () {
    const client = await pool.connect()
    clients.push(client)

    return client
  }

  return {
    get client () {
      return client
    },

    createClient,

    async inTransaction (fn, suppliedClient = client) {
      return inTransaction(suppliedClient, fn)
    },

    get pool () {
      return pool
    },

    get query () {
      return query
    },
  }
}
