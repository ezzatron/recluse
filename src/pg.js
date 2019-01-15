const Cursor = require('pg-cursor')

const UNIQUE_VIOLATION = '23505'

module.exports = {
  asyncQuery,
  waitForNotification,

  UNIQUE_VIOLATION,
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
  }
}

async function waitForNotification (client, channel) {
  await client.query(`LISTEN ${channel}`)
  let notification

  try {
    notification = await new Promise((resolve, reject) => {
      function onEnd (error) {
        removeListeners()
        reject(error)
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
  } finally {
    await client.query(`UNLISTEN ${channel}`)
  }

  return notification
}

function createCursorIterator (cursor) {
  let done = false

  return {
    next () {
      return new Promise((resolve, reject) => {
        if (done) return resolve({done})

        cursor.read(1, (error, rows, result) => {
          if (error) return reject(error)

          done = rows.length < 1

          if (done) return resolve({done, value: result})

          resolve({done, value: rows[0]})
        })
      })
    },
  }
}
