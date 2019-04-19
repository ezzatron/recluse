const {asyncQuery, continuousQuery, UNIQUE_VIOLATION} = require('./pg.js')

module.exports = {
  appendEvents,
  readEvents,
  readEventsByStream,
  readEventsContinuously,
}

const EVENT_CHANNEL = 'recluse_event'

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

  await pgClient.query(`NOTIFY ${EVENT_CHANNEL}`)

  return true
}

function readEvents (pgClient, offset = 0) {
  return pgClient.query(asyncQuery(
    'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset',
    [offset],
    marshal
  ))
}

function readEventsByStream (pgClient, name, offset = 0) {
  if (!name) throw new Error('Invalid stream name')

  return pgClient.query(asyncQuery(
    `
    SELECT e.* FROM recluse.event AS e
    INNER JOIN recluse.stream AS s ON s.id = e.stream_id
    WHERE s.name = $1 AND e.stream_offset >= $2
    ORDER BY e.stream_offset
    `,
    [name, offset],
    marshal
  ))
}

function readEventsContinuously (pgClient, options = {}) {
  const {clock, offset, timeout} = options

  return continuousQuery(
    pgClient,
    'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset',
    EVENT_CHANNEL,
    ({globalOffset}) => globalOffset + 1,
    {clock, marshal, offset, timeout}
  )
}

async function insertEvent (pgClient, offset, streamId, streamOffset, event) {
  const {type, data = null} = event

  await pgClient.query(
    'INSERT INTO recluse.event (global_offset, type, stream_id, stream_offset, data) VALUES ($1, $2, $3, $4, $5)',
    [offset, type, streamId, streamOffset, data]
  )
}

async function insertStream (pgClient, type, name, next) {
  let result

  try {
    result = await pgClient.query(
      'INSERT INTO recluse.stream (type, name, next) VALUES ($1, $2, $3) RETURNING id',
      [type, name, next]
    )
  } catch (error) {
    if (error.code === UNIQUE_VIOLATION) return [false, null]

    throw error
  }

  return [true, result.rows[0].id]
}

async function updateGlobalOffset (pgClient, count) {
  const result = await pgClient.query(
    `
    INSERT INTO recluse.global_offset AS go (next) VALUES ($1)
    ON CONFLICT (id) DO UPDATE SET next = go.next + $1
    RETURNING next
    `,
    [count]
  )

  return result.rows[0].next - count
}

async function updateStreamOffset (pgClient, name, start, next) {
  const result = await pgClient.query(
    'UPDATE recluse.stream SET next = $1 WHERE name = $2 AND next = $3 RETURNING id',
    [next, name, start]
  )

  return result.rowCount > 0 ? [true, result.rows[0].id] : [false, null]
}

function marshal (row) {
  const {
    data,
    global_offset: globalOffset,
    stream_id: streamId,
    stream_offset: streamOffset,
    time,
    type,
  } = row

  return {
    event: data === null ? {type} : {type, data},
    globalOffset: parseInt(globalOffset),
    streamId: parseInt(streamId),
    streamOffset: parseInt(streamOffset),
    time,
  }
}
