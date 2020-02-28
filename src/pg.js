const Cursor = require('pg-cursor')

module.exports = {
  acquireSessionLock,
  asyncQuery,
}

/**
 * Acquires an advisory lock for a particular namespace / ID combination.
 *
 * Returns an async function that should be called to release the lock.
 */
async function acquireSessionLock (context, logger, pool, namespace, id = 0) {
  const client = await pool.connect()

  try {
    await context.do(async () => client.query('SELECT pg_advisory_lock($1, $2)', [namespace, id]))

    return async function releaseSessionLock (context) {
      try {
        await context.do(async () => client.query('SELECT pg_advisory_unlock($1, $2)', [namespace, id]))
        client.release()
      } catch (error) {
        client.release(true)

        throw error
      }
    }
  } catch (error) {
    client.release(true)

    throw error
  }
}

/**
 * Returns an async function that will return query results one at a time until
 * exhausted.
 *
 * The query is not executed until the first read.
 */
function asyncQuery (logger, pool, text, values, marshal = identity) {
  const cursor = new Cursor(text, values)
  let isDone = false
  let client

  return async function readRow (context) {
    if (isDone) return [true, undefined]

    if (!client) {
      try {
        client = await pool.connect()
        await context.do(async () => client.query(cursor))
      } catch (error) {
        client.release(true)

        throw error
      }
    }

    let result

    try {
      result = await context.doPromise((resolve, reject) => {
        cursor.read(1, (error, rows) => {
          if (error) return reject(error)
          if (rows.length < 1) return resolve([true, undefined])

          resolve([false, marshal(rows[0])])
        })
      })
    } catch (error) {
      isDone = true

      await new Promise(resolve => {
        cursor.close(error => {
          if (error) logger.warn(`Unable to close cursor: ${error.stack}`)

          resolve()
        })
      })

      client.release(true)

      throw error
    }

    if (result[0]) {
      isDone = true

      try {
        await new Promise((resolve, reject) => {
          cursor.close(error => {
            if (error) return reject(error)

            resolve()
          })
        })

        client.release()
      } catch (error) {
        client.release(true)

        throw error
      }
    }

    return result
  }
}

function identity (value) {
  return value
}
