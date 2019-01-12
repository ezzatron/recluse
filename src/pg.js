const Cursor = require('pg-cursor')

module.exports = {
  asyncQuery,
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
