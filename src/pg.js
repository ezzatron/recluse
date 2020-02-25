const {Pool, types} = require('pg')
const Cursor = require('pg-cursor')

const {CancelError, createCancelController, createTimeout, TimeoutError} = require('./async.js')
const {systemClock} = require('./clock.js')
const {acquireAsyncIterator} = require('./iterator.js')

const UNIQUE_VIOLATION = '23505'

module.exports = {
  acquireSessionLock,
  asyncQuery,
  configure,
  continuousQuery,
  createPool,
  inTransaction,
  releaseSessionLock,
  waitForNotification,

  UNIQUE_VIOLATION,
}

async function acquireSessionLock (pgClient, namespace, id = 0) {
  await pgClient.query('SELECT pg_advisory_lock($1, $2)', [namespace, id])
}

function asyncQuery (text, values, marshal) {
  const cursor = new Cursor(text, values)
  const iterator = createCursorIterator(cursor, marshal)

  return {
    handleCommandComplete: cursor.handleCommandComplete.bind(cursor),
    handleDataRow: cursor.handleDataRow.bind(cursor),
    handleError: cursor.handleError.bind(cursor),
    handlePortalSuspended: cursor.handlePortalSuspended.bind(cursor),
    handleReadyForQuery: cursor.handleReadyForQuery.bind(cursor),
    handleRowDescription: cursor.handleRowDescription.bind(cursor),
    submit: cursor.submit.bind(cursor),

    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

const TYPE_TIMESTAMP = 1114
const TYPE_TIMESTAMPTZ = 1184

function configure () {
  const noParse = v => v

  types.setTypeParser(TYPE_TIMESTAMP, noParse)
  types.setTypeParser(TYPE_TIMESTAMPTZ, noParse)
}

function continuousQuery (logger, pgClient, text, channel, nextOffset, options = {}) {
  const {extraValues = [], marshal = identity, start = 0, timeout = 100, clock = systemClock} = options
  const iterator = createContinuousQueryIterator(
    logger,
    pgClient,
    text,
    start,
    nextOffset,
    extraValues,
    marshal,
    channel,
    timeout,
    clock,
  )

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

function createPool (config = {}) {
  return new Pool(config)
}

async function inTransaction (pgClient, fn) {
  let result

  await pgClient.query('BEGIN')

  try {
    result = await fn()
  } catch (error) {
    await pgClient.query('ROLLBACK')

    throw error
  }

  await pgClient.query('COMMIT')

  return result
}

async function releaseSessionLock (pgClient, namespace, id = 0) {
  await pgClient.query('SELECT pg_advisory_unlock($1, $2)', [namespace, id])
}

async function waitForNotification (client, channel) {
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

function createCursorIterator (cursor, marshal = identity) {
  const cancelController = createCancelController()
  let done = false
  let final

  return {
    async next () {
      try {
        if (done) return {done, value: final}

        const [rows, result] = await cancelController.race(cursorRead(cursor, 1))
        done = rows.length < 1

        if (!done) return {done, value: marshal(rows[0])}

        final = result
        await cancelController.race(cursorClose(cursor))

        return {done, value: final}
      } catch (error) {
        if (error instanceof CancelError) {
          return {done: true}
        } else {
          throw error
        }
      }
    },

    async cancel () {
      cancelController.cancel()

      await cursorClose(cursor)
    },
  }
}

function createContinuousQueryIterator (
  logger,
  pgClient,
  text,
  start,
  nextOffset,
  extraValues,
  marshal,
  channel,
  timeout,
  clock,
) {
  const cancelController = createCancelController()
  let next = start
  let isListening = false
  let iterator = null
  let nextNotification = null

  return {
    async next () {
      try {
        while (true) {
          if (cancelController.isCancelled) return {done: true}

          if (!isListening) {
            logger.debug(`Listening for Postgres notifications on ${channel}`)
            await cancelController.race(pgClient.query(`LISTEN ${channel}`))
            isListening = true
          }

          if (!iterator) {
            if (!nextNotification) {
              nextNotification = waitForNotification(pgClient, channel)
                .then(() => {
                  nextNotification = null
                })
                .catch(() => {}) // if this rejects, we just attempt another query anyway
            }

            logger.debug('Creating async query iterator')
            iterator = acquireAsyncIterator(pgClient.query(asyncQuery(text, [next, ...extraValues], marshal)))
          }

          logger.debug('Awaiting next async query result')
          const result = await cancelController.race(iterator.next())

          if (result.done) {
            logger.debug('Reached end of async query results')
          } else {
            logger.debug('Returning received async query result')
            next = nextOffset(result.value)

            return result
          }

          iterator = null
          const waitFor = [nextNotification]

          if (typeof timeout === 'number') {
            waitFor.push(createTimeout(clock, timeout))
            logger.debug(`Awaiting Postgres notification on ${channel}, or for ${timeout}ms to elapse`)
          } else {
            logger.debug(`Awaiting Postgres notification on ${channel}`)
          }

          try {
            await cancelController.race(...waitFor)
            logger.debug(`Received Postgres notification on ${channel}`)
          } catch (error) {
            if (error instanceof TimeoutError) {
              logger.debug(`${timeout}ms elapsed`)
            } else {
              throw error
            }
          }
        }
      } catch (error) {
        if (error instanceof CancelError) {
          logger.debug('Detected cancellation of continuous query')

          return {done: true}
        } else {
          throw error
        }
      }
    },

    async cancel () {
      const errors = []

      logger.debug('Cancelling command handler')
      cancelController.cancel()

      if (iterator) {
        logger.debug('Cancelling async query iterator')

        try {
          await iterator.cancel()
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to cancel async query iterator: ${error.stack}`)
        }
      } else {
        logger.debug('No async query iterator to cancel')
      }

      if (isListening) {
        logger.debug(`Un-listening to Postgres notifications on ${channel}`)

        try {
          await pgClient.query(`UNLISTEN ${channel}`)
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to un-listen to Postgres notifications on ${channel}: ${error.stack}`)
        }
      } else {
        logger.debug(`Never listened for Postgres notifications on ${channel}`)
      }

      if (errors.length > 0) throw errors[0]
    },
  }
}

function cursorRead (cursor, rowCount) {
  return new Promise((resolve, reject) => {
    cursor.read(rowCount, (error, rows, result) => {
      if (error) return reject(error)

      resolve([rows, result])
    })
  })
}

function cursorClose (cursor) {
  return new Promise((resolve, reject) => {
    cursor.close(error => { error ? reject(error) : resolve() })
  })
}

function identity (value) {
  return value
}
