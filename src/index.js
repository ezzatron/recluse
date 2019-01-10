const {asyncQuery} = require('./pg.js')

module.exports = {
  appendEvents,
  initializeSchema,
  readEvents,
}

const UNIQUE_VIOLATION = '23505'

async function initializeSchema (pgClient) {
  await pgClient.query(`
    CREATE SCHEMA IF NOT EXISTS recluse
  `)

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS recluse.global_offset
    (
      id bool DEFAULT TRUE,
      next bigint NOT NULL,

      PRIMARY KEY (id),
      CONSTRAINT one_row CHECK (id)
    )
  `)

  await pgClient.query(`
    CREATE SEQUENCE IF NOT EXISTS recluse.stream_id_seq AS bigint MINVALUE 0
  `)

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS recluse.stream
    (
      id bigint NOT NULL DEFAULT nextval('recluse.stream_id_seq'),
      type text NOT NULL,
      name text NOT NULL,
      next bigint NOT NULL,

      PRIMARY KEY (id),
      UNIQUE (name)
    )
  `)

  await pgClient.query(`
    ALTER SEQUENCE recluse.stream_id_seq OWNED BY recluse.stream.id
  `)

  await pgClient.query(`
    CREATE INDEX IF NOT EXISTS idx_stream_type ON recluse.stream (type)
  `)

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS recluse.event
    (
      global_offset bigint NOT NULL,
      type text NOT NULL,
      stream_id bigint NOT NULL,
      stream_offset bigint NOT NULL,
      data bytea NOT NULL,
      time timestamp NOT NULL DEFAULT now(),

      PRIMARY KEY (global_offset),
      UNIQUE (stream_id, stream_offset),
      FOREIGN KEY (stream_id) REFERENCES recluse.stream (id)
    )
  `)
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

async function updateStreamOffset (pgClient, name, start, next) {
  const result = await pgClient.query(
    'UPDATE recluse.stream SET next = $1 WHERE name = $2 AND next = $3 RETURNING id',
    [next, name, start]
  )

  return result.rowCount > 0 ? [true, result.rows[0].id] : [false, null]
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

async function insertEvent (pgClient, offset, streamId, streamOffset, event) {
  const {type, data} = event

  await pgClient.query(
    'INSERT INTO recluse.event (global_offset, type, stream_id, stream_offset, data) VALUES ($1, $2, $3, $4, $5)',
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

function readEvents (pgClient, name, offset = 0) {
  return pgClient.query(asyncQuery(
    `
    SELECT e.* FROM recluse.event AS e
    INNER JOIN recluse.stream AS s ON s.id = e.stream_id
    WHERE s.name = $1 AND e.stream_offset >= $2
    ORDER BY e.stream_offset
    `,
    [name, offset]
  ))
}
