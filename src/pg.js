const {doInterminable, withDefer} = require('./async.js')

module.exports = {
  withAdvisoryLock,
}

/**
 * Uses an advisory lock for a particular namespace / ID combination to perform
 * a unit of work.
 */
async function withAdvisoryLock (context, logger, pool, namespace, id, fn) {
  return withClient(context, logger, pool, async client => withDefer(async defer => {
    defer(await acquireAdvisoryLock(context, logger, client, namespace, id))

    return fn()
  }))
}

async function withClient (context, logger, pool, fn) {
  return withDefer(async defer => {
    const client = await acquireClient(context, logger, pool)
    defer(recover => client.release(recover()))

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
