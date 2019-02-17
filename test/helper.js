/* eslint-disable mocha/no-top-level-hooks */

const {Client, Pool} = require('pg')

const {inTransaction} = require('../src/pg.js')

module.exports = {
  asyncIterableToArray,
  pgSpec,
  resolveOnCallback,
}

function pgSpec (spec) {
  return function () {
    before(async function () {
      this.pgInitClient = createClient('postgres')

      await this.pgInitClient.connect()
      await this.pgInitClient.query('DROP DATABASE IF EXISTS recluse_test')
      await this.pgInitClient.query('CREATE DATABASE recluse_test')

      this.pgPool = createPool('recluse_test')
    })

    beforeEach(async function () {
      this.pgClients = []
      this.createPgClient = async () => {
        const client = await this.pgPool.connect()
        this.pgClients.push(client)

        return client
      }

      this.pgClient = await this.createPgClient()
      this.query = this.pgClient.query.bind(this.pgClient)
      this.inTransaction = async (fn, pgClient = this.pgClient) => inTransaction(pgClient, fn)
    })

    afterEach(async function () {
      for (const client of this.pgClients) {
        try {
          client.release()
        } catch (error) {}
      }

      await this.pgClient.query('DROP SCHEMA recluse CASCADE')
    })

    after(async function () {
      try {
        await this.pgPool.end()
      } catch (error) {}

      await this.pgInitClient.query('DROP DATABASE recluse_test')

      try {
        await this.pgInitClient.end()
      } catch (error) {}
    })

    spec.call(this)
  }
}

const {PGHOST, PGPORT, PGUSER, PGPASSWORD} = process.env
const connectOptions = {
  host: PGHOST || 'localhost',
  port: parseInt(PGPORT || '5432'),
  user: PGUSER || 'postgres',
  password: PGPASSWORD || '',
}

function createClient (database) {
  return new Client({...connectOptions, database})
}

function createPool (database) {
  return new Pool({...connectOptions, database})
}

function resolveOnCallback () {
  let resolver
  const promise = new Promise(resolve => { resolver = resolve })

  return [promise, resolver]
}

async function asyncIterableToArray (iterable) {
  if (iterable == null) throw new Error('Not an object')

  const iteratorFactory = iterable[Symbol.asyncIterator]

  if (typeof iteratorFactory !== 'function') throw new Error('Not an async iterable')

  const iterator = iteratorFactory()
  const array = []
  let returnValue
  let value, done

  do {
    ({value, done} = await iterator.next())

    if (done) {
      returnValue = value
    } else {
      array.push(value)
    }
  } while (!done)

  return [array, returnValue]
}
