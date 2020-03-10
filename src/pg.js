const Cursor = require('pg-cursor')

const {doInterminable, withDefer} = require('./async.js')

module.exports = {
  consumeQuery,
  withAdvisoryLock,
}

/**
 * Executes a query and feeds the rows one-by-one into a consumer function.
 *
 * The consumer function should return a boolean to indicate whether to continue
 * consuming rows.
 */
async function consumeQuery (context, logger, pool, text, values, fn) {
  return withDefer(async defer => {
    return withClient(context, logger, pool, async client => {
      const cursor = await createCursor(context, logger, client, text, values)
      defer(() => closeCursor(context, logger, cursor))

      let shouldContinue = true

      while (shouldContinue) {
        const row = await readFromCursor(context, logger, cursor)
        shouldContinue = row && await fn(row)
      }
    })
  })
}

/**
 * Uses an advisory lock for a particular namespace / ID combination to perform
 * a unit of work.
 */
async function withAdvisoryLock (context, logger, pool, namespace, id, fn) {
  return withDefer(async defer => {
    return withClient(context, logger, pool, async client => {
      defer(await acquireAdvisoryLock(context, logger, client, namespace, id))

      return fn()
    })
  })
}

async function withClient (context, logger, pool, fn) {
  return withDefer(async defer => {
    const client = await acquireClient(context, logger, pool)
    defer(recover => {
      const error = recover()

      if (error) {
        client.release(true)

        throw error
      }

      client.release()
    })

    return fn(client)
  })
}

async function acquireAdvisoryLock (context, logger, client, namespace, id) {
  async function releaseAdvisoryLock () {
    await client.query('SELECT pg_advisory_unlock($1, $2)', [namespace, id])
  }

  return doInterminable(
    context,
    async () => {
      await client.query('SELECT pg_advisory_lock($1, $2)', [namespace, id])

      return releaseAdvisoryLock
    },
    async promise => {
      try {
        await promise
      } catch (error) {
        logger.debug(`Postgres advisory lock acquisition failed during cleanup: ${error.stack}`)

        return // lock was never acquired
      }

      try {
        await releaseAdvisoryLock()
      } catch (error) {
        logger.warn(`Unable to cleanly release Postgres advisory lock: ${error.stack}`)
      }
    },
  )
}

async function acquireClient (context, logger, pool) {
  return doInterminable(
    context,
    async () => pool.connect(),
    async promise => {
      let client

      try {
        client = await promise
      } catch (error) {
        logger.debug(`Postgres client acquisition failed during cleanup: ${error.stack}`)

        return // client was never acquired
      }

      try {
        client.release()
      } catch (error) {
        logger.warn(`Unable to cleanly release Postgres client: ${error.stack}`)
      }
    },
  )
}

async function createCursor (context, logger, client, text, values) {
  return doInterminable(
    context,
    async () => client.query(new Cursor(text, values)),
    async promise => {
      let cursor

      try {
        cursor = await promise
      } catch (error) {
        logger.debug(`Postgres cursor query failed during cleanup: ${error.stack}`)

        return // query was never submitted
      }

      try {
        await closeCursorAsync(logger, cursor)
      } catch (error) {
        logger.warn(`Unable to cleanly close Postgres cursor: ${error.stack}`)
      }
    },
  )
}

async function closeCursor (context, logger, cursor) {
  return doInterminable(context, () => closeCursorAsync(logger, cursor))
}

function closeCursorAsync (logger, cursor) {
  return new Promise((resolve, reject) => {
    cursor.close(error => {
      if (error) return reject(error)

      resolve()
    })
  })
}

async function readFromCursor (context, logger, cursor) {
  return doInterminable(context, () => readFromCursorAsync(logger, cursor))
}

function readFromCursorAsync (logger, cursor) {
  return new Promise((resolve, reject) => {
    cursor.read(1, (error, rows) => {
      if (error) return reject(error)

      resolve(rows[0])
    })
  })
}
