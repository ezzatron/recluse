const {asyncQuery, continuousQuery, UNIQUE_VIOLATION} = require('./pg.js')
const {EVENT: CHANNEL} = require('./channel.js')

module.exports = {
  appendEvents,
  appendEventsUnchecked,
  readEvents,
  readEventsByStream,
  readEventsContinuously,
  readNextStreamOffset,
}

async function appendEvents (pgClient, type, instance, start, events) {
  const count = events.length
  const next = start + count

  const [isUpdated, streamId] = start === 0
    ? await insertStream(pgClient, type, instance, next)
    : await updateStreamOffset(pgClient, type, instance, start, next)

  if (!isUpdated) return false

  const offset = await updateGlobalOffset(pgClient, count)

  for (let i = 0; i < count; ++i) {
    await insertEvent(pgClient, offset + i, streamId, start + i, events[i])
  }

  await pgClient.query(`NOTIFY ${CHANNEL}`)

  return true
}

async function appendEventsUnchecked (pgClient, type, instance, events) {
  const count = events.length

  const [streamId, start] = await updateStreamOffsetUnchecked(pgClient, type, instance, count)
  const offset = await updateGlobalOffset(pgClient, count)

  for (let i = 0; i < count; ++i) {
    await insertEvent(pgClient, offset + i, streamId, start + i, events[i])
  }

  await pgClient.query(`NOTIFY ${CHANNEL}`)

  return true
}

function readEvents (pgClient, start = 0, end = Infinity) {
  let text, params

  if (isFinite(end)) {
    text = 'SELECT * FROM recluse.event WHERE global_offset >= $1 AND global_offset < $2 ORDER BY global_offset'
    params = [start, end]
  } else {
    text = 'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset'
    params = [start]
  }

  return pgClient.query(asyncQuery(text, params, marshal))
}

function readEventsByStream (pgClient, type, instance, start = 0, end = Infinity) {
  if (typeof type !== 'string') throw new Error('Invalid stream type')
  if (typeof instance !== 'string') throw new Error('Invalid stream instance')

  let text, params

  if (isFinite(end)) {
    text = `
      SELECT e.* FROM recluse.event AS e
      INNER JOIN recluse.stream AS s ON s.id = e.stream_id
      WHERE s.type = $1 AND s.instance = $2 AND e.stream_offset >= $3 AND e.stream_offset < $4
      ORDER BY e.stream_offset
      `
    params = [type, instance, start, end]
  } else {
    text = `
      SELECT e.* FROM recluse.event AS e
      INNER JOIN recluse.stream AS s ON s.id = e.stream_id
      WHERE s.type = $1 AND s.instance = $2 AND e.stream_offset >= $3
      ORDER BY e.stream_offset
      `
    params = [type, instance, start]
  }

  return pgClient.query(asyncQuery(text, params, marshal))
}

function readEventsContinuously (pgClient, options = {}) {
  const {clock, start, timeout} = options

  return continuousQuery(
    pgClient,
    'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset',
    CHANNEL,
    ({globalOffset}) => globalOffset + 1,
    {clock, marshal, start, timeout}
  )
}

async function readNextStreamOffset (pgClient, type, instance) {
  if (typeof type !== 'string') throw new Error('Invalid stream type')
  if (typeof instance !== 'string') throw new Error('Invalid stream instance')

  const result = await pgClient.query(
    'SELECT next FROM recluse.stream WHERE type = $1 AND instance = $2',
    [type, instance]
  )

  if (result.rowCount < 1) return 0

  return parseInt(result.rows[0].next)
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

async function updateStreamOffset (pgClient, type, instance, start, next) {
  const result = await pgClient.query(
    'UPDATE recluse.stream SET next = $1 WHERE type = $2 AND instance = $3 AND next = $4 RETURNING id',
    [next, type, instance, start]
  )

  return result.rowCount > 0 ? [true, result.rows[0].id] : [false, null]
}

async function updateStreamOffsetUnchecked (pgClient, type, instance, count) {
  const result = await pgClient.query(
    `
    INSERT INTO recluse.stream AS s (type, instance, next) VALUES ($1, $2, $3)
    ON CONFLICT (type, instance) DO UPDATE SET next = s.next + $3
    RETURNING id, next
    `,
    [type, instance, count]
  )
  const {id, next} = result.rows[0]

  return [id, next - count]
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
