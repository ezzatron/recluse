const Cursor = require('pg-cursor')
const {str: crc32} = require('crc-32')

const UNIQUE_VIOLATION = '23505'
const LOCK_NAMESPACE = crc32('recluse')

module.exports = {
  acquireSessionLock,
  asyncQuery,
  inTransaction,
  releaseSessionLock,
  waitForNotification,

  UNIQUE_VIOLATION,
}

async function acquireSessionLock (pgClient, name) {
  await pgClient.query('SELECT pg_advisory_lock($1, $2)', [LOCK_NAMESPACE, crc32(name)])
}

function asyncQuery (text, values) {
  const cursor = new Cursor(text, values)
  const iterator = createCursorIterator(cursor)

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

async function releaseSessionLock (pgClient, name) {
  await pgClient.query('SELECT pg_advisory_unlock($1, $2)', [LOCK_NAMESPACE, crc32(name)])
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

function createCursorIterator (cursor) {
  let done = false
  let final

  return {
    async next () {
      if (done) return {done, value: final}

      const [rows, result] = await cursorRead(cursor, 1)
      done = rows.length < 1

      if (!done) return {done, value: rows[0]}

      final = result

      return {done, value: final}
    },

    async cancel () {
      await cursorClose(cursor)
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
