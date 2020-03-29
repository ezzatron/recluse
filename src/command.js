const {COMMAND: CHANNEL} = require('./channel.js')
const {createLazyGetter} = require('./object.js')
const {consumeContinuousQuery, consumeQuery, query} = require('./pg.js')

module.exports = {
  executeCommands,
  readCommands,
  readUnhandledCommandsContinuously,
}

/**
 * Execute a list of commands.
 */
async function executeCommands (context, logger, client, serialization, source, commands) {
  const {serialize} = serialization

  for (const command of commands) {
    const {type, data} = command

    await query(
      context,
      logger,
      client,
      'INSERT INTO recluse.command (source, type, data) VALUES ($1, $2, $3)',
      [source, type, serialize(data)],
    )
  }

  await query(context, logger, client, `NOTIFY ${CHANNEL}`)
}

/**
 * Reads commands and feeds the rows one-by-one into a consumer function.
 */
async function readCommands (context, logger, pool, serialization, id, fn) {
  return consumeQuery(
    context,
    logger,
    pool,
    'SELECT * FROM recluse.command WHERE id >= $1 ORDER BY id',
    {values: [id]},
    async row => fn(marshal(serialization, row)),
  )
}

/**
 * Reads unhandled commands continuously  and feeds the rows one-by-one into a
 * consumer function.
 */
function readUnhandledCommandsContinuously (context, logger, pool, serialization, options, fn) {
  const {timeout} = options

  return consumeContinuousQuery(
    context,
    logger,
    pool,
    CHANNEL,
    ({id}) => id + 1,
    'SELECT * FROM recluse.command WHERE handled_at IS NULL AND id >= $1 ORDER BY id',
    {timeout},
    async row => fn(marshal(serialization, row)),
  )
}

function marshal (serialization, row) {
  const {unserialize} = serialization

  const {
    data,
    executed_at: executedAt,
    handled_at: handledAt,
    id,
    source,
    type,
  } = row

  const command = {type}
  createLazyGetter(command, 'data', () => unserialize(data))

  return {
    command,
    executedAt,
    handledAt,
    id: parseInt(id),
    source,
  }
}
