module.exports = {
  initializeSchema,
}

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
