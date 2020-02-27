const {Client, Pool} = require('pg')

const {inTransaction} = require('../../src/pg.js')

module.exports = {
  createTestHelper,

  TIME_PATTERN: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+\+00$/,
}

function createTestHelper (userAfterEach) {
  const {PGHOST, PGPORT, PGUSER, PGPASSWORD} = process.env
  const config = {
    host: PGHOST || 'localhost',
    port: parseInt(PGPORT || '5432'),
    user: PGUSER || 'postgres',
    password: PGPASSWORD || '',
    database: `recluse_test_${Math.random().toString(36).substring(2, 15)}`,
  }

  const clients = []
  const schemas = []
  let client, initClient, pool, query

  beforeAll(async function pgTestHelperBeforeAll () {
    initClient = new Client({...config, database: 'postgres'})

    await initClient.connect()
    await initClient.query(`DROP DATABASE IF EXISTS ${config.database}`)
    await initClient.query(`CREATE DATABASE ${config.database}`)

    pool = new Pool(config)
  })

  beforeEach(async function pgTestHelperBeforeEach () {
    client = await createClient()
    query = client.query.bind(client)
  })

  afterEach(async function pgTestHelperAfterEach () {
    if (userAfterEach) await userAfterEach()

    for (const client of clients) {
      try {
        client.release()
      } catch (error) {}
    }

    await Promise.allSettled(schemas.map(schema => initClient.query(`DROP SCHEMA ${schema} CASCADE`)))
  })

  afterAll(async function pgTestHelperAfterAll () {
    try {
      await pool.end()
    } catch (error) {}

    await initClient.query(`DROP DATABASE ${config.database}`)

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

    get config () {
      return config
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

    trackSchemas (...toAdd) {
      schemas.push(...toAdd)
    },

    trackTempSchema (prefix) {
      const schema = `${prefix}_${Math.random().toString(36).substring(2, 15)}`
      schemas.push(schema)

      return schema
    },
  }
}
