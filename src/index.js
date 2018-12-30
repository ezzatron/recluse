const QueryStream = require('pg-query-stream')

const UNIQUE_VIOLATION = '23505'

async function initializeSchema (pgClient) {
  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS offset
    (
      next bigint NOT NULL,

      UNIQUE ((1))
    )
  `)

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS stream
    (
      id bigserial NOT NULL,
      type text NOT NULL,
      name text NOT NULL,
      next bigint NOT NULL,

      PRIMARY KEY (id),
      UNIQUE (name)
    )
  `)

  await pgClient.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_type ON stream (type)
  `)

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS event
    (
      offset bigint NOT NULL,
      type text NOT NULL,
      stream_id bigint NOT NULL,
      stream_offset bigint NOT NULL,
      data bytea NOT NULL,
      time timestamp NOT NULL DEFAULT now(),

      PRIMARY KEY (offset),
      UNIQUE (stream_id, stream_offset),
      FOREIGN KEY (stream_id) REFERENCES stream (id)
    )
  `)
}

async function insertStream (pgClient, type, name, next) {
  try {
    const result = await pgClient.query(
      'INSERT INTO stream (type, name, next) VALUES ($1, $2, $3) RETURNING id',
      [type, name, next]
    )
  } catch (error) {
    if (error.code === UNIQUE_VIOLATION) return [false, null]

    throw error
  }

  return [true, result.rows[0].id]
}

async function updateStreamOffset (pgClient, name, start, next) {
  const result = await pgClient.query(
    'UPDATE stream SET next = $1 WHERE name = $2 AND next = $3 RETURNING id',
    [next, name, start]
  )

  return result.rowCount > 0 ? [true, result.rows[0].id] : [false, null]
}

async function updateGlobalOffset (pgClient, count) {
  const result = await pgClient.query(
    'INSERT INTO offset (next) VALUES ($1) ON CONFLICT DO UPDATE SET next = next + $1 RETURNING next',
    [count]
  )

  return result.rows[0].next - count
}

async function insertEvent (pgClient, offset, streamId, streamOffset, event) {
  const {type, data} = event

  await pgClient.query(
    'INSERT INTO event (offset, type, stream_id, stream_offset, data) VALUES ($1, $2, $3, $4, $5)',
    [offset, type, streamId, streamOffset, data]
  )
}

async function appendEvents (pgClient, type, name, start, events) {
  const count = events.length
  const next = start + count

  const [isUpdated, streamId] = start === 0
    ? await insertStream(pgClient, type, name, next)
    : await updateStreamOffset(pgClient, name, start, next)

  if (!isUpdated) return false

  const offset = await updateGlobalOffset(pgClient, count)

  for (let i = 0; i < count; ++i) {
    await insertEvent(pgClient, offset + i, streamId, start + i, events[i])
  }

  return true
}

async function readEvents (pgClient, callback, name, offset = 0) {
  return streamQuery(
    pgClient,
    `
    SELECT e.* FROM event AS e
    INNER JOIN stream AS s ON s.id = e.stream_id
    WHERE s.name = $1 AND e.stream_offset >= $2
    ORDER BY e.stream_offset
    `,
    [name, offset],
    callback
  )
}

function streamQuery (pgClient, query, parameters, callback) {
  return new Promise ((resolve, reject) => {
    const stream = pgClient.query(new QueryStream(query, parameters))

    stream.on('data', callback)
    stream.on('error', handleError)
    stream.on('end', handleEnd)
    stream.on('close', handleEnd)

    function handleError (error) {
      removeHandlers()
      reject(error)
    }

    function handleEnd () {
      removeHandlers()
      resolve()
    }

    function removeHandlers () {
      stream.removeHandler('data', callback)
      stream.removeHandler('error', handleError)
      stream.removeHandler('end', handleEnd)
      stream.removeHandler('close', handleEnd)
    }
  })
}

async function inTransaction (pgClient, fn) {
  await pgClient.query('BEGIN')

  try {
    await fn()
  } catch (error) {
    await pgClient.query('ROLLBACK')

    throw error
  }

  await pgClient.query('COMMIT')
}
