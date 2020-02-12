module.exports = {
  initializeSchema,
}

async function initializeSchema (pgClient) {
  await pgClient.query('CREATE SCHEMA IF NOT EXISTS bank')

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS bank.account
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
  await pgClient.query('CREATE INDEX IF NOT EXISTS name_idx ON bank.account (name)')
  await pgClient.query('CREATE INDEX IF NOT EXISTS balance_idx ON bank.account (balance)')
  await pgClient.query('CREATE INDEX IF NOT EXISTS pending_balance_idx ON bank.account (pending_balance)')

  await pgClient.query(`
    CREATE TABLE IF NOT EXISTS bank.transaction
    (
      id text NOT NULL,
      type text NOT NULL,
      status text NOT NULL DEFAULT 'PENDING',
      amount bigint NOT NULL,
      to_id text DEFAULT NULL REFERENCES bank.account(id) ON UPDATE CASCADE ON DELETE CASCADE,
      from_id text DEFAULT NULL REFERENCES bank.account(id) ON UPDATE CASCADE ON DELETE CASCADE,

      PRIMARY KEY (id)
    )
  `)
  await pgClient.query('CREATE INDEX IF NOT EXISTS type_idx ON bank.transaction (type)')
  await pgClient.query('CREATE INDEX IF NOT EXISTS status_idx ON bank.transaction (status)')
  await pgClient.query('CREATE INDEX IF NOT EXISTS amount_idx ON bank.transaction (amount)')
  await pgClient.query('CREATE INDEX IF NOT EXISTS to_idx ON bank.transaction (to_id, from_id)')
  await pgClient.query('CREATE INDEX IF NOT EXISTS from_idx ON bank.transaction (from_id, to_id)')
}
