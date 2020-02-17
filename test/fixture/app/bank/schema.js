module.exports = {
  initializeSchema,
}

async function initializeSchema (pgClient, schemaName) {
  await pgClient.query(`CREATE SCHEMA IF NOT EXISTS ${schemaName}`)

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS ${schemaName}.account
    (
      id text NOT NULL,
      name text NOT NULL DEFAULT '',
      balance bigint NOT NULL DEFAULT 0,
      pending_balance bigint NOT NULL DEFAULT 0,
      deposits_in bigint NOT NULL DEFAULT 0,
      withdrawals_out bigint NOT NULL DEFAULT 0,
      transfers_in bigint NOT NULL DEFAULT 0,
      transfers_out bigint NOT NULL DEFAULT 0,

      PRIMARY KEY (id)
    )
  `)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS name_idx ON ${schemaName}.account (name)`)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS balance_idx ON ${schemaName}.account (balance)`)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS pending_balance_idx ON ${schemaName}.account (pending_balance)`)

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS ${schemaName}.transaction
    (
      id text NOT NULL,
      type text NOT NULL,
      status text NOT NULL DEFAULT 'PENDING',
      amount bigint NOT NULL,
      to_id text DEFAULT NULL REFERENCES ${schemaName}.account(id) ON UPDATE CASCADE ON DELETE CASCADE,
      from_id text DEFAULT NULL REFERENCES ${schemaName}.account(id) ON UPDATE CASCADE ON DELETE CASCADE,

      PRIMARY KEY (id)
    )
  `)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS type_idx ON ${schemaName}.transaction (type)`)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS status_idx ON ${schemaName}.transaction (status)`)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS amount_idx ON ${schemaName}.transaction (amount)`)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS to_idx ON ${schemaName}.transaction (to_id, from_id)`)
  await pgClient.query(`CREATE INDEX IF NOT EXISTS from_idx ON ${schemaName}.transaction (from_id, to_id)`)
}
