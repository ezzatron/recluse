const Cursor = require('pg-cursor')
const {types} = require('pg')

const {acquireAsyncIterator} = require('./iterator.js')
const {allSerial} = require('./async.js')
const {systemClock} = require('./clock.js')

const UNIQUE_VIOLATION = '23505'

module.exports = {
  acquireSessionLock,
  asyncQuery,
  configure,
  continuousQuery,
  inTransaction,
  releaseSessionLock,
  waitForNotification,

  UNIQUE_VIOLATION,
}

async function acquireSessionLock (pgClient, namespace, id) {
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

function continuousQuery (pgClient, text, channel, nextOffset, options = {}) {
  const {extraValues = [], marshal = identity, offset = 0, timeout = 100, clock = systemClock} = options
  const iterator =
    createContinuousQueryIterator(pgClient, text, offset, nextOffset, extraValues, marshal, channel, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
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

async function releaseSessionLock (pgClient, namespace, id) {
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
  let done = false
  let final

  return {
    async next () {
      if (done) return {done, value: final}

      const [rows, result] = await cursorRead(cursor, 1)
      done = rows.length < 1

      if (!done) return {done, value: marshal(rows[0])}

      final = result
      await cursorClose(cursor)

      return {done, value: final}
    },

    async cancel () {
      await cursorClose(cursor)
    },
  }
}

function createContinuousQueryIterator (
  pgClient,
  text,
  offset,
  nextOffset,
  extraValues,
  marshal,
  channel,
  timeout,
  clock
) {
  let next = offset
  let isListening = false
  let iterator = null
  let nextNotification = null
  let timeoutId = null

  return {
    async next () {
      while (true) {
        if (!isListening) {
          await pgClient.query(`LISTEN ${channel}`)
          isListening = true
        }

        if (!iterator) {
          // if this rejects, we just attempt another query anyway
          nextNotification = waitForNotification(pgClient, channel).catch(() => {})

          iterator = acquireAsyncIterator(pgClient.query(asyncQuery(text, [next, ...extraValues], marshal)))
        }

        const result = await iterator.next()

        if (!result.done) {
          next = nextOffset(result.value)

          return result
        }

        iterator = null

        if (typeof timeout === 'number') {
          const timeoutPromise = new Promise(resolve => {
            timeoutId = clock.setTimeout(() => {
              timeoutId = null
              resolve()
            }, timeout)
          })

          await Promise.race([nextNotification, timeoutPromise])
        } else {
          await nextNotification
        }
      }
    },

    async cancel () {
      await allSerial(
        async () => { if (iterator) await iterator.cancel() },
        async () => { if (isListening) await pgClient.query(`UNLISTEN ${channel}`) },
        () => { if (timeoutId) clock.clearTimeout(timeoutId) }
      )
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
