const {asyncQuery, continuousQuery} = require('./pg.js')
const {COMMAND: CHANNEL} = require('./channel.js')
const {COMMAND} = require('./handler.js')
const {createLazyGetter} = require('./object.js')

module.exports = {
  executeCommands,
  readCommands,
  readUnhandledCommandsContinuously,
}

async function executeCommands (serialization, pgClient, source, commands) {
  const {serialize} = serialization

  for (const command of commands) {
    const {type, data} = command

    await pgClient.query(
      'INSERT INTO recluse.command (source, type, data) VALUES ($1, $2, $3)',
      [source, type, serialize(data, COMMAND, type)]
    )
  }

  await pgClient.query(`NOTIFY ${CHANNEL}`)
}

function readCommands (serialization, pgClient, id = 0) {
  const {unserialize} = serialization

  return pgClient.query(asyncQuery(
    'SELECT * FROM recluse.command WHERE id >= $1 ORDER BY id',
    [id],
    marshal.bind(null, unserialize)
  ))
}

function readUnhandledCommandsContinuously (serialization, pgClient, options = {}) {
  const {unserialize} = serialization
  const {clock, timeout} = options

  return continuousQuery(
    pgClient,
    'SELECT * FROM recluse.command WHERE handled_at IS NULL AND id >= $1 ORDER BY id',
    CHANNEL,
    ({id}) => id + 1,
    {
      clock,
      marshal: marshal.bind(null, unserialize),
      timeout,
    }
  )
}

function marshal (unserialize, row) {
  const {
    data,
    executed_at: executedAt,
    handled_at: handledAt,
    id,
    source,
    type,
  } = row

  const command = {type}
  createLazyGetter(command, 'data', () => unserialize(data, COMMAND, type))

  return {
    command,
    executedAt,
    handledAt,
    id: parseInt(id),
    source,
  }
}
