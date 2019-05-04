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

// transaction types
const DEPOSIT = 'DEPOSIT'
const TRANSFER = 'TRANSFER'
const WITHDRAWAL = 'WITHDRAWAL'

// transaction statuses
const COMPLETE = 'COMPLETE'
const DECLINED = 'DECLINED'
const PENDING = 'PENDING'

async function applyEvent (pgClient, event) {
  const {type, data} = event

  switch (type) {
    case ACCOUNT_OPENED: return createAccount(pgClient, data)

    case DEPOSIT_STARTED: return createDeposit(pgClient, data)
    case ACCOUNT_CREDITED_FOR_DEPOSIT: return completeDeposit(pgClient, data)

    case WITHDRAWAL_STARTED: return createWithdrawal(pgClient, data)
    case WITHDRAWAL_DECLINED_DUE_TO_INSUFFICIENT_FUNDS: return declineWithdrawal(pgClient, data)
    case ACCOUNT_DEBITED_FOR_WITHDRAWAL: return completeWithdrawal(pgClient, data)

    case TRANSFER_STARTED: return createTransfer(pgClient, data)
    case TRANSFER_DECLINED_DUE_TO_INSUFFICIENT_FUNDS: return declineTransfer(pgClient, data)
    case ACCOUNT_CREDITED_FOR_TRANSFER: return completeTransfer(pgClient, data)
  }
}

async function createAccount (pgClient, data) {
  const {accountId, name} = data

  await pgClient.query(
    `
    INSERT INTO bank.account AS a (id, name) VALUES ($1, $2)
    ON CONFLICT (id) DO UPDATE SET name = $2 WHERE a.name = ''
    `,
    [accountId, name]
  )
}

async function createDeposit (pgClient, data) {
  const {accountId, amount, transactionId} = data

  const deltas = {pendingBalance: amount}
  const to = [accountId, deltas]

  await updateTransaction(pgClient, null, PENDING, DEPOSIT, transactionId, amount, {to})
}

async function completeDeposit (pgClient, data) {
  const {accountId, amount, transactionId} = data

  const deltas = {balance: amount, depositsIn: amount}
  const to = [accountId, deltas]

  await updateTransaction(pgClient, PENDING, COMPLETE, DEPOSIT, transactionId, amount, {to})
}

async function createWithdrawal (pgClient, data) {
  const {accountId, amount, transactionId} = data

  const deltas = {pendingBalance: -amount}
  const from = [accountId, deltas]

  await updateTransaction(pgClient, null, PENDING, WITHDRAWAL, transactionId, amount, {from})
}

async function declineWithdrawal (pgClient, data) {
  const {accountId, amount, transactionId} = data

  const deltas = {pendingBalance: amount}
  const from = [accountId, deltas]

  await updateTransaction(pgClient, PENDING, DECLINED, WITHDRAWAL, transactionId, amount, {from})
}

async function completeWithdrawal (pgClient, data) {
  const {accountId, amount, transactionId} = data

  const deltas = {balance: -amount, withdrawalsOut: amount}
  const from = [accountId, deltas]

  await updateTransaction(pgClient, PENDING, COMPLETE, WITHDRAWAL, transactionId, amount, {from})
}

async function createTransfer (pgClient, data) {
  const {amount, fromAccountId, toAccountId, transactionId} = data

  const fromDeltas = {pendingBalance: -amount}
  const toDeltas = {pendingBalance: amount}

  const from = [fromAccountId, fromDeltas]
  const to = [toAccountId, toDeltas]

  await updateTransaction(pgClient, null, PENDING, TRANSFER, transactionId, amount, {from, to})
}

async function declineTransfer (pgClient, data) {
  const {amount, fromAccountId, toAccountId, transactionId} = data

  const fromDeltas = {pendingBalance: amount}
  const toDeltas = {pendingBalance: -amount}

  const from = [fromAccountId, fromDeltas]
  const to = [toAccountId, toDeltas]

  await updateTransaction(pgClient, PENDING, DECLINED, TRANSFER, transactionId, amount, {from, to})
}

async function completeTransfer (pgClient, data) {
  const {amount, fromAccountId, toAccountId, transactionId} = data

  const fromDeltas = {balance: -amount, transfersOut: amount}
  const toDeltas = {balance: amount, transfersIn: amount}

  const from = [fromAccountId, fromDeltas]
  const to = [toAccountId, toDeltas]

  await updateTransaction(pgClient, PENDING, COMPLETE, TRANSFER, transactionId, amount, {from, to})
}

async function updateTransaction (pgClient, previousStatus, status, type, transactionId, amount, accounts) {
  const {from, to} = accounts
  const [fromAccountId, fromDeltas] = from || []
  const [toAccountId, toDeltas] = to || []

  const result = await pgClient.query(
    `
    INSERT INTO bank.transaction AS t (status, type, id, from_id, to_id, amount) VALUES ($2, $3, $4, $5, $6, $7)
    ON CONFLICT (id) DO UPDATE SET status = $2 WHERE t.status = $1
    `,
    [previousStatus, status, type, transactionId, fromAccountId, toAccountId, amount]
  )

  if (result.rowCount < 1) return

  if (from) await updateAccount(pgClient, fromAccountId, fromDeltas)
  if (to) await updateAccount(pgClient, toAccountId, toDeltas)
}

async function updateAccount (pgClient, accountId, deltas) {
  const {
    balance = 0,
    pendingBalance = 0,
    depositsIn = 0,
    withdrawalsOut = 0,
    transfersIn = 0,
    transfersOut = 0,
  } = deltas

  await pgClient.query(
    `
    INSERT INTO bank.account AS a
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
    ]
  )
}

module.exports = {
  applyEvent,
}
