const {inPoolTransaction, query} = require('./pg.js')

module.exports = {
  initializeSchema,
}

/**
 * Initialize the Recluse schema.
 */
async function initializeSchema (context, logger, pool) {
  return inPoolTransaction(context, logger, pool, async client => {
    const clientQuery = query.bind(null, context, logger, client)

    await clientQuery('CREATE SCHEMA IF NOT EXISTS recluse')

    await clientQuery('CREATE SEQUENCE IF NOT EXISTS recluse.commandIdSeq AS bigint MINVALUE 0')
    await clientQuery(`
      CREATE TABLE IF NOT EXISTS recluse.command
      (
        id bigint NOT NULL DEFAULT nextval('recluse.commandIdSeq'),
        type text NOT NULL,
        data bytea DEFAULT NULL,
        source text NOT NULL,
        executed_at timestamp with time zone NOT NULL DEFAULT now(),
        handled_at timestamp with time zone DEFAULT NULL,

        PRIMARY KEY (id)
      )
    `)
    await clientQuery('ALTER SEQUENCE recluse.commandIdSeq OWNED BY recluse.command.id')
  })
}
