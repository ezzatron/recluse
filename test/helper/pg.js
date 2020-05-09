const {userInfo} = require('os')
const {Client, Pool} = require('pg')

module.exports = {
  createTestHelper,

  TIME_PATTERN: /^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d.\d+\+\d\d$/,
}

function createTestHelper (options = {}) {
  const {
    afterEach: userAfterEach,
    beforeEach: userBeforeEach,
  } = options

  const {PGHOST, PGPORT, PGUSER, PGPASSWORD} = process.env
  const config = {
    host: PGHOST || 'localhost',
    port: parseInt(PGPORT || '5432'),
    user: PGUSER || userInfo().username,
    password: PGPASSWORD || '',
    database: `test_${Math.random().toString(36).substring(2, 15)}`,
  }

  let pool, schemas

  const helper = {
    async inTransaction (fn) {
      const client = new Client(config)
      await client.connect()

      try {
        let result

        await client.query('BEGIN')

        try {
          result = await fn(client)
        } catch (error) {
          await client.query('ROLLBACK')

          throw error
        }

        await client.query('COMMIT')

        return result
      } finally {
        await client.end()
      }
    },

    get pool () {
      return pool
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

  beforeAll(async function pgTestHelperBeforeAll () {
    const client = new Client({...config, database: 'postgres'})
    await client.connect()
    await client.query(`CREATE DATABASE ${config.database}`)
    await client.end()
  })

  beforeEach(async function pgTestHelperBeforeEach () {
    schemas = []
    pool = new Pool(config)

    if (userBeforeEach) await userBeforeEach(helper)
  })

  afterEach(async function pgTestHelperAfterEach () {
    if (userAfterEach) await userAfterEach(helper)

    await pool.end()

    if (schemas.length > 0) {
      const client = new Client(config)
      await client.connect()

      try {
        await inTransaction(client, async () => {
          for (const schema of schemas) {
            await client.query(`DROP SCHEMA ${schema} CASCADE`)
          }
        })
      } finally {
        await client.end()
      }
    }
  })

  afterAll(async function pgTestHelperAfterAll () {
    const client = new Client({...config, database: 'postgres'})
    await client.connect()
    await client.query(`DROP DATABASE ${config.database}`)
    await client.end()
  })

  return helper
}

async function inTransaction (client, fn) {
  let result

  await client.query('BEGIN')

  try {
    result = await fn()
  } catch (error) {
    await client.query('ROLLBACK')

    throw error
  }

  await client.query('COMMIT')

  return result
}
