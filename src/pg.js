const {Pool, types} = require('pg')
const Cursor = require('pg-cursor')

const {systemClock} = require('./clock.js')
const {Canceled, createContext, TimedOut} = require('./context.js')
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

function asyncQuery (context, text, values, marshal) {
  const cursor = new Cursor(text, values)
  const iterator = createCursorIterator(context, cursor, marshal)

  return {
    handleCommandComplete: cursor.handleCommandComplete.bind(cursor),
    handleDataRow: cursor.handleDataRow.bind(cursor),
    handleError: cursor.handleError.bind(cursor),
    handlePortalSuspended: cursor.handlePortalSuspended.bind(cursor),
    handleReadyForQuery: cursor.handleReadyForQuery.bind(cursor),
    handleRowDescription: cursor.handleRowDescription.bind(cursor),
    submit: cursor.submit.bind(cursor),

    [Symbol.asyncIterator]: () => iterator,
  }
}

const TYPE_TIMESTAMP = 1114
const TYPE_TIMESTAMPTZ = 1184

function configure () {
  const noParse = v => v

  types.setTypeParser(TYPE_TIMESTAMP, noParse)
  types.setTypeParser(TYPE_TIMESTAMPTZ, noParse)
}

function continuousQuery (context, logger, pgClient, text, channel, nextOffset, options = {}) {
  const {extraValues = [], marshal = identity, start = 0, timeout = 100, clock = systemClock} = options
  const iterator = createContinuousQueryIterator(
    context,
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

function createCursorIterator (context, cursor, marshal = identity) {
  let done = false
  let final

  return {
    async next () {
      try {
        if (done) return {done, value: final}

        const [rows, result] = await cursorRead(context, cursor, 1)
        done = rows.length < 1

        if (!done) return {done, value: marshal(rows[0])}

        final = result
        await cursorClose(context, cursor)

        return {done, value: final}
      } catch (error) {
        if (error instanceof Canceled) {
          return {done: true}
        }

        throw error
      }
    },
  }
}

function createContinuousQueryIterator (
  context,
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
  let next = start
  let isListening = false
  let iterator = null
  let nextNotification = null

  return {
    async next () {
      try {
        while (true) {
          if (!isListening) {
            isListening = true

            await context.do(async () => {
              logger.debug(`Listening for Postgres notifications on ${channel}`)

              await pgClient.query(`LISTEN ${channel}`)
              await context.onceDone(() => pgClient.query(`UNLISTEN ${channel}`))
            })
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
            iterator = acquireAsyncIterator(pgClient.query(asyncQuery(context, text, [next, ...extraValues], marshal)))
          }

          logger.debug('Awaiting next async query result')
          const result = await iterator.next()

          if (result.done) {
            logger.debug('Reached end of async query results')
          } else {
            logger.debug('Returning received async query result')
            next = nextOffset(result.value)

            return result
          }

          iterator = null
          const notificationContext = createContext({clock, context, timeout})

          try {
            await notificationContext.do(() => nextNotification)
            logger.debug(`Received Postgres notification on ${channel}`)
          } catch (error) {
            if (error instanceof TimedOut) {
              logger.debug(`${timeout}ms elapsed`)
            }

            throw error
          } finally {
            await notificationContext.cancel()
          }
        }
      } catch (error) {
        if (error instanceof Canceled) {
          logger.debug('Detected cancellation of continuous query')

          return {done: true}
        }

        throw error
      }
    },
  }
}

function cursorRead (context, cursor, rowCount) {
  return context.promise((resolve, reject) => {
    cursor.read(rowCount, (error, rows, result) => {
      if (error) return reject(error)

      resolve([rows, result])
    })
  })
}

function cursorClose (context, cursor) {
  return context.promise((resolve, reject) => {
    cursor.close(error => { error ? reject(error) : resolve() })
  })
}

function identity (value) {
  return value
}
