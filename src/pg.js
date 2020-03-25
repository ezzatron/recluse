const Cursor = require('pg-cursor')

const {assertRunning, createContext, doInterminable, isTimedOut, withDefer} = require('./async.js')

module.exports = {
  consumeContinuousQuery,
  consumeQuery,
  inTransaction,
  withAdvisoryLock,
}

/**
 * Continuously executes a query and feeds the rows one-by-one into a consumer
 * function.
 *
 * A channel name must be provided. This channel will be listened to for new
 * notifications, which indicate that more rows are ready to read.
 *
 * The nextOffset function will be passed each row, and should return the next
 * offset to feed back into the query. This offset is used as the first query
 * parameter.
 *
 * The consumer function should return a boolean to indicate whether to continue
 * consuming rows.
 */
async function consumeContinuousQuery (context, logger, pool, channel, nextOffset, text, options, fn) {
  return withDefer(async defer => {
    return withClient(context, logger, pool, async client => {
      const {start = 0, timeout = 100, values = []} = options

      await query(context, logger, client, `LISTEN ${channel}`)
      defer(async recover => {
        const error = recover()
        if (error) throw error

        await query(context, logger, client, `UNLISTEN ${channel}`)
      })

      let shouldContinue = true
      let offset = start
      let nextNotification, notificationContext

      while (shouldContinue) {
        assertRunning(context)

        if (!nextNotification) {
          notificationContext = createContext(logger, {context, timeout})
          nextNotification = waitForNotification(notificationContext, logger, client, channel)
            .catch(() => {}) // if this rejects, we just attempt another query anyway
            .then(() => { nextNotification = null })
        }

        const options = {values: [offset, ...values]}
        shouldContinue = await consumeQuery(context, logger, pool, text, options, row => {
          offset = nextOffset(row)

          return fn(row)
        })

        if (!shouldContinue) break

        try {
          await nextNotification
        } catch (error) {
          if (!isTimedOut(error)) throw error
        }
      }
    })
  })
}

/**
 * Executes a query and feeds the rows one-by-one into a consumer function.
 *
 * The consumer function should return a boolean to indicate whether to continue
 * consuming rows.
 *
 * Returns a boolean indicating whether the query was completely consumed.
 */
async function consumeQuery (context, logger, pool, text, options, fn) {
  return withDefer(async defer => {
    return withClient(context, logger, pool, async client => {
      const {values = []} = options

      const cursor = await createCursor(context, logger, client, text, values)
      defer(() => closeCursor(context, logger, cursor))

      let shouldContinue = true

      while (shouldContinue) {
        const row = await readFromCursor(context, logger, cursor)

        if (!row) return true

        shouldContinue = await fn(row)
      }

      return false
    })
  })
}

/**
 * Executes a function while maintaining a transaction.
 *
 * The transaction will either be committed when the call resolves, or rolled
 * back when the call rejects.
 */
async function inTransaction (context, logger, client, fn) {
  return withDefer(async defer => {
    await query(context, logger, client, 'BEGIN')

    defer(async recover => {
      const error = recover()

      if (error) {
        await query(context, logger, client, 'ROLLBACK')

        throw error
      }

      await query(context, logger, client, 'COMMIT')
    })

    return fn()
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

async function query (context, logger, client, text, values) {
  return doInterminable(context, () => client.query(text, values))
}

async function waitForNotification (context, logger, client, channel) {
  return doInterminable(context, () => waitForNotificationAsync(client, channel))
}

function waitForNotificationAsync (client, channel) {
  return new Promise((resolve, reject) => {
    function onEnd (error) {
      removeListeners()
      reject(error || new Error('Client disconnected while waiting for notification'))
    }

    function onNotification (notification) {
      if (notification.channel !== channel) return

      removeListeners()
      resolve(notification)
    }

    function removeListeners () {
      client.removeListener('end', onEnd)
      client.removeListener('error', onEnd)
      client.removeListener('notification', onNotification)
    }

    client.on('end', onEnd)
    client.on('error', onEnd)
    client.on('notification', onNotification)
  })
}
