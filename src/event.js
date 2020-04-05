const {EVENT: CHANNEL} = require('./channel.js')
const {createLazyGetter} = require('./object.js')
const {consumeContinuousQuery, consumeQuery, query, UNIQUE_VIOLATION} = require('./pg.js')

module.exports = {
  appendEvents,
  appendEventsUnchecked,
  readEvents,
  readEventsByStream,
  readEventsContinuously,
  readNextStreamOffset,
}

async function appendEvents (context, logger, client, serialization, type, instance, start, events) {
  const count = events.length
  const next = start + count

  const [isUpdated, streamId] = start === 0
    ? await insertStream(context, logger, client, type, instance, next)
    : await updateStreamOffset(context, logger, client, type, instance, start, next)

  if (!isUpdated) return false

  const offset = await updateGlobalOffset(context, logger, client, count)

  for (let i = 0; i < count; ++i) {
    await insertEvent(context, logger, client, serialization, offset + i, streamId, start + i, events[i])
  }

  await query(context, logger, client, `NOTIFY ${CHANNEL}`)

  return true
}

async function appendEventsUnchecked (context, logger, client, serialization, type, instance, events) {
  const count = events.length

  const [streamId, start] = await updateStreamOffsetUnchecked(context, logger, client, type, instance, count)
  const offset = await updateGlobalOffset(context, logger, client, count)

  for (let i = 0; i < count; ++i) {
    await insertEvent(context, logger, client, serialization, offset + i, streamId, start + i, events[i])
  }

  await query(context, logger, client, `NOTIFY ${CHANNEL}`)
}

async function readEvents (context, logger, client, serialization, start, end, fn) {
  let text, values

  if (isFinite(end)) {
    text = 'SELECT * FROM recluse.event WHERE global_offset >= $1 AND global_offset < $2 ORDER BY global_offset'
    values = [start, end]
  } else {
    text = 'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset'
    values = [start]
  }

  return consumeQuery(
    context,
    logger,
    client,
    text,
    {values},
    async row => fn(marshal(serialization, row)),
  )
}

async function readEventsByStream (context, logger, client, serialization, type, instance, start, end, fn) {
  if (typeof type !== 'string') throw new Error('Invalid stream type')
  if (typeof instance !== 'string') throw new Error('Invalid stream instance')

  let text, values

  if (isFinite(end)) {
    text = `
      SELECT e.* FROM recluse.event AS e
      INNER JOIN recluse.stream AS s ON s.id = e.stream_id
      WHERE s.type = $1 AND s.instance = $2 AND e.stream_offset >= $3 AND e.stream_offset < $4
      ORDER BY e.stream_offset
      `
    values = [type, instance, start, end]
  } else {
    text = `
      SELECT e.* FROM recluse.event AS e
      INNER JOIN recluse.stream AS s ON s.id = e.stream_id
      WHERE s.type = $1 AND s.instance = $2 AND e.stream_offset >= $3
      ORDER BY e.stream_offset
      `
    values = [type, instance, start]
  }

  return consumeQuery(
    context,
    logger,
    client,
    text,
    {values},
    async row => fn(marshal(serialization, row)),
  )
}

async function readEventsContinuously (context, logger, pool, serialization, options, fn) {
  const {start, timeout} = options

  return consumeContinuousQuery(
    context,
    logger,
    pool,
    CHANNEL,
    ({globalOffset}) => globalOffset + 1,
    'SELECT * FROM recluse.event WHERE global_offset >= $1 ORDER BY global_offset',
    {start, timeout},
    async row => fn(marshal(serialization, row)),
  )
}

async function readNextStreamOffset (context, logger, client, type, instance) {
  if (typeof type !== 'string') throw new Error('Invalid stream type')
  if (typeof instance !== 'string') throw new Error('Invalid stream instance')

  const result = await query(
    context,
    logger,
    client,
    'SELECT next FROM recluse.stream WHERE type = $1 AND instance = $2',
    [type, instance],
  )

  if (result.rowCount < 1) return 0

  return parseInt(result.rows[0].next)
}

async function insertEvent (context, logger, client, serialization, offset, streamId, streamOffset, event) {
  const {serialize} = serialization
  const {type, data} = event

  await query(
    context,
    logger,
    client,
    'INSERT INTO recluse.event (global_offset, type, stream_id, stream_offset, data) VALUES ($1, $2, $3, $4, $5)',
    [offset, type, streamId, streamOffset, serialize(data)],
  )
}

async function insertStream (context, logger, client, type, instance, next) {
  let result

  try {
    result = await query(
      context,
      logger,
      client,
      'INSERT INTO recluse.stream (type, instance, next) VALUES ($1, $2, $3) RETURNING id',
      [type, instance, next],
    )
  } catch (error) {
    if (error.code === UNIQUE_VIOLATION) return [false, null]

    throw error
  }

  return [true, result.rows[0].id]
}

async function updateStreamOffset (context, logger, client, type, instance, start, next) {
  const result = await query(
    context,
    logger,
    client,
    'UPDATE recluse.stream SET next = $1 WHERE type = $2 AND instance = $3 AND next = $4 RETURNING id',
    [next, type, instance, start],
  )

  return result.rowCount > 0 ? [true, result.rows[0].id] : [false, null]
}

async function updateStreamOffsetUnchecked (context, logger, client, type, instance, count) {
  const result = await query(
    context,
    logger,
    client,
    `
    INSERT INTO recluse.stream AS s (type, instance, next) VALUES ($1, $2, $3)
    ON CONFLICT (type, instance) DO UPDATE SET next = s.next + $3
    RETURNING id, next
    `,
    [type, instance, count],
  )
  const {id, next} = result.rows[0]

  return [id, next - count]
}

async function updateGlobalOffset (context, logger, client, count) {
  const result = await query(
    context,
    logger,
    client,
    `
    INSERT INTO recluse.global_offset AS go (next) VALUES ($1)
    ON CONFLICT (id) DO UPDATE SET next = go.next + $1
    RETURNING next
    `,
    [count],
  )

  return result.rows[0].next - count
}

function marshal (serialization, row) {
  const {unserialize} = serialization
  const {
    data,
    global_offset: globalOffset,
    stream_id: streamId,
    stream_offset: streamOffset,
    time,
    type,
  } = row

  const event = {type}
  createLazyGetter(event, 'data', () => unserialize(data))

  return {
    event,
    globalOffset: parseInt(globalOffset),
    streamId: parseInt(streamId),
    streamOffset: parseInt(streamOffset),
    time,
  }
}
