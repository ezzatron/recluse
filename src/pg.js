const Cursor = require('pg-cursor')

module.exports = {
  asyncQuery,
}

function asyncQuery (text, values) {
  const cursor = new Cursor(text, values)
  const iterator = createCursorIterator(cursor)

  return {
    submit (connection) {
      cursor.submit(connection)
    },

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
