const Cursor = require('pg-cursor')

const {createContext} = require('./async.js')

module.exports = {
  acquireSessionLock,
  asyncQuery,
}

/**
 * Acquires an advisory lock for a particular namespace / ID combination.
 *
 * The lock is released when the supplied context is done.
 */
async function acquireSessionLock (context, logger, client, namespace, id = 0) {
  await context.do(async () => {
    logger.debug(`Acquiring session lock for ${namespace}.${id}`)
    await client.query('SELECT pg_advisory_lock($1, $2)', [namespace, id])
    logger.debug(`Acquired session lock for ${namespace}.${id}`)

    await context.onceDone(() => client.query('SELECT pg_advisory_unlock($1, $2)', [namespace, id]))
  })
}

/**
 * Returns an async function that will return query results one at a time until
 * exhausted.
 *
 * The query is not executed until the first read.
 */
function asyncQuery (logger, client, text, values, marshal = identity) {
  let cursor, cursorContext
  let isDone = false

  return async function readAsyncQueryRow (context) {
    if (isDone) return [true, undefined]

    if (!cursor) {
      cursorContext = await createContext(logger, {context})
      cursor = client.query(
        await cursorContext.do(async () => {
          const cursor = new Cursor(text, values)

          await cursorContext.onceDone(() => new Promise((resolve, reject) => {
            cursor.close(error => {
              if (error) return reject(error)

              resolve()
            })
          }))

          return cursor
        }),
      )
    }

    let result

    try {
      result = await context.doPromise((resolve, reject) => {
        cursor.read(1, (error, rows) => {
          if (error) return reject(error)

          const isEmpty = rows.length < 1
          const row = isEmpty ? undefined : marshal(rows[0])

          resolve([isEmpty, row])
        })
      })
    } catch (error) {
      await cursorContext.cancel()

      throw error
    }

    if (result[0]) {
      isDone = true
      await cursorContext.cancel()
    }

    return result
  }
}

function identity (value) {
  return value
}
