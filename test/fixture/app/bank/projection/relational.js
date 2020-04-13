const {
  ACCOUNT_CREDITED_FOR_DEPOSIT,
  ACCOUNT_CREDITED_FOR_TRANSFER,
  ACCOUNT_DEBITED_FOR_WITHDRAWAL,
  ACCOUNT_OPENED,
  DEPOSIT_STARTED,
  TRANSFER_DECLINED_DUE_TO_INSUFFICIENT_FUNDS,
  TRANSFER_STARTED,
  WITHDRAWAL_DECLINED_DUE_TO_INSUFFICIENT_FUNDS,
  WITHDRAWAL_STARTED,
} = require('../event.js')

module.exports = function createProjection (schemaName) {
  // transaction types
  const DEPOSIT = 'DEPOSIT'
  const TRANSFER = 'TRANSFER'
  const WITHDRAWAL = 'WITHDRAWAL'

  // transaction statuses
  const COMPLETE = 'COMPLETE'
  const DECLINED = 'DECLINED'
  const PENDING = 'PENDING'

  return {
    async applyEvent (scope) {
      const {client, event: {type, data}} = scope

      switch (type) {
        case ACCOUNT_OPENED: return createAccount(client, data)

        case DEPOSIT_STARTED: return createDeposit(client, data)
        case ACCOUNT_CREDITED_FOR_DEPOSIT: return completeDeposit(client, data)

        case WITHDRAWAL_STARTED: return createWithdrawal(client, data)
        case WITHDRAWAL_DECLINED_DUE_TO_INSUFFICIENT_FUNDS: return declineWithdrawal(client, data)
        case ACCOUNT_DEBITED_FOR_WITHDRAWAL: return completeWithdrawal(client, data)

        case TRANSFER_STARTED: return createTransfer(client, data)
        case TRANSFER_DECLINED_DUE_TO_INSUFFICIENT_FUNDS: return declineTransfer(client, data)
        case ACCOUNT_CREDITED_FOR_TRANSFER: return completeTransfer(client, data)
      }
    },
  }

  async function createAccount (client, data) {
    const {accountId, name} = data

    await client.query(
      `
      INSERT INTO ${schemaName}.account AS a (id, name) VALUES ($1, $2)
      ON CONFLICT (id) DO UPDATE SET name = $2 WHERE a.name = ''
      `,
      [accountId, name],
    )
  }

  async function createDeposit (client, data) {
    const {accountId, amount, transactionId} = data

    const deltas = {pendingBalance: amount}
    const to = [accountId, deltas]

    await updateTransaction(client, null, PENDING, DEPOSIT, transactionId, amount, {to})
  }

  async function completeDeposit (client, data) {
    const {accountId, amount, transactionId} = data

    const deltas = {balance: amount, depositsIn: amount}
    const to = [accountId, deltas]

    await updateTransaction(client, PENDING, COMPLETE, DEPOSIT, transactionId, amount, {to})
  }

  async function createWithdrawal (client, data) {
    const {accountId, amount, transactionId} = data

    const deltas = {pendingBalance: -amount}
    const from = [accountId, deltas]

    await updateTransaction(client, null, PENDING, WITHDRAWAL, transactionId, amount, {from})
  }

  async function declineWithdrawal (client, data) {
    const {accountId, amount, transactionId} = data

    const deltas = {pendingBalance: amount}
    const from = [accountId, deltas]

    await updateTransaction(client, PENDING, DECLINED, WITHDRAWAL, transactionId, amount, {from})
  }

  async function completeWithdrawal (client, data) {
    const {accountId, amount, transactionId} = data

    const deltas = {balance: -amount, withdrawalsOut: amount}
    const from = [accountId, deltas]

    await updateTransaction(client, PENDING, COMPLETE, WITHDRAWAL, transactionId, amount, {from})
  }

  async function createTransfer (client, data) {
    const {amount, fromAccountId, toAccountId, transactionId} = data

    const fromDeltas = {pendingBalance: -amount}
    const toDeltas = {pendingBalance: amount}

    const from = [fromAccountId, fromDeltas]
    const to = [toAccountId, toDeltas]

    await updateTransaction(client, null, PENDING, TRANSFER, transactionId, amount, {from, to})
  }

  async function declineTransfer (client, data) {
    const {accountId: fromAccountId, amount, transactionId} = data

    const fromDeltas = {pendingBalance: amount}
    const toDeltas = {pendingBalance: -amount}

    const result = await client.query(`SELECT to_id FROM ${schemaName}.transaction WHERE id = $1`, [transactionId])

    if (result.rowCount < 1) throw new Error('Unable to correlate transfer for declining')

    const {to_id: toAccountId} = result.rows[0]

    const from = [fromAccountId, fromDeltas]
    const to = [toAccountId, toDeltas]

    await updateTransaction(client, PENDING, DECLINED, TRANSFER, transactionId, amount, {from, to})
  }

  async function completeTransfer (client, data) {
    const {accountId: toAccountId, amount, transactionId} = data

    const fromDeltas = {balance: -amount, transfersOut: amount}
    const toDeltas = {balance: amount, transfersIn: amount}

    const result = await client.query(`SELECT from_id FROM ${schemaName}.transaction WHERE id = $1`, [transactionId])

    if (result.rowCount < 1) throw new Error('Unable to correlate transfer for completion')

    const {from_id: fromAccountId} = result.rows[0]

    const from = [fromAccountId, fromDeltas]
    const to = [toAccountId, toDeltas]

    await updateTransaction(client, PENDING, COMPLETE, TRANSFER, transactionId, amount, {from, to})
  }

  async function updateTransaction (client, previousStatus, status, type, transactionId, amount, accounts) {
    const {from, to} = accounts
    const [fromAccountId, fromDeltas] = from || []
    const [toAccountId, toDeltas] = to || []

    if (from) await updateAccount(client, fromAccountId, fromDeltas)
    if (to) await updateAccount(client, toAccountId, toDeltas)

    await client.query(
      `
      INSERT INTO ${schemaName}.transaction AS t (status, type, id, from_id, to_id, amount)
      VALUES ($2, $3, $4, $5, $6, $7)
      ON CONFLICT (id) DO UPDATE SET status = $2 WHERE t.status = $1
      `,
      [previousStatus, status, type, transactionId, fromAccountId, toAccountId, amount],
    )
  }

  async function updateAccount (client, accountId, deltas) {
    const {
      balance = 0,
      pendingBalance = 0,
      depositsIn = 0,
      withdrawalsOut = 0,
      transfersIn = 0,
      transfersOut = 0,
    } = deltas

    await client.query(
      `
      INSERT INTO ${schemaName}.account AS a
      (
        id,
        balance,
        pending_balance,
        deposits_in,
        withdrawals_out,
        transfers_in,
        transfers_out
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (id) DO UPDATE SET
        balance = a.balance + $2,
        pending_balance = a.pending_balance + $3,
        deposits_in = a.deposits_in + $4,
        withdrawals_out = a.withdrawals_out + $5,
        transfers_in = a.transfers_in + $6,
        transfers_out = a.transfers_out + $7
      `,
      [
        accountId,
        balance,
        pendingBalance,
        depositsIn,
        withdrawalsOut,
        transfersIn,
        transfersOut,
      ],
    )
  }
}
