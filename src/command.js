const {COMMAND: CHANNEL} = require('./channel.js')
const {createLazyGetter} = require('./object.js')
const {consumeQuery, inPoolTransaction} = require('./pg.js')

module.exports = {
  executeCommands,
  readCommands,
}

async function executeCommands (context, logger, pool, serialization, source, commands) {
  return inPoolTransaction(context, logger, pool, async client => {
    const {serialize} = serialization

    for (const command of commands) {
      const {type, data} = command

      await client.query(
        'INSERT INTO recluse.command (source, type, data) VALUES ($1, $2, $3)',
        [source, type, serialize(data)],
      )
    }

    await client.query(`NOTIFY ${CHANNEL}`)
  })
}

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
