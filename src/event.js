const {asyncQuery, continuousQuery, UNIQUE_VIOLATION} = require('./pg.js')
const {EVENT: CHANNEL} = require('./channel.js')

module.exports = {
  appendEvents,
  readEvents,
  readEventsByStream,
  readEventsContinuously,
}

async function appendEvents (pgClient, type, instance, start, events) {
  const count = events.length
  const next = start + count

  const [isUpdated, streamId] = start === 0
    ? await insertStream(pgClient, type, instance, next)
    : await updateStreamOffset(pgClient, instance, start, next)

  if (!isUpdated) return false

  const offset = await updateGlobalOffset(pgClient, count)

  for (let i = 0; i < count; ++i) {
    await insertEvent(pgClient, offset + i, streamId, start + i, events[i])
  }

  await pgClient.query(`NOTIFY ${CHANNEL}`)

  return true
}

function readEvents (pgClient, offset = 0) {
  return pgClient.query(asyncQuery(
    'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset',
    [offset],
    marshal
  ))
}

function readEventsByStream (pgClient, instance, offset = 0) {
  if (!instance) throw new Error('Invalid stream instance')

  return pgClient.query(asyncQuery(
    `
    SELECT e.* FROM recluse.event AS e
    INNER JOIN recluse.stream AS s ON s.id = e.stream_id
    WHERE s.instance = $1 AND e.stream_offset >= $2
    ORDER BY e.stream_offset
    `,
    [instance, offset],
    marshal
  ))
}

function readEventsContinuously (pgClient, options = {}) {
  const {clock, offset, timeout} = options

  return continuousQuery(
    pgClient,
    'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset',
    CHANNEL,
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

async function insertStream (pgClient, type, instance, next) {
  let result

  try {
    result = await pgClient.query(
      'INSERT INTO recluse.stream (type, instance, next) VALUES ($1, $2, $3) RETURNING id',
      [type, instance, next]
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

async function updateStreamOffset (pgClient, instance, start, next) {
  const result = await pgClient.query(
    'UPDATE recluse.stream SET next = $1 WHERE instance = $2 AND next = $3 RETURNING id',
    [next, instance, start]
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
