const {
  CREDIT_ACCOUNT_FOR_DEPOSIT,
  CREDIT_ACCOUNT_FOR_TRANSFER,
  DEBIT_ACCOUNT_FOR_TRANSFER,
  DEBIT_ACCOUNT_FOR_WITHDRAWAL,
  OPEN_ACCOUNT,
} = require('../command.js')

const {
  ACCOUNT_CREDITED_FOR_DEPOSIT,
  ACCOUNT_CREDITED_FOR_TRANSFER,
  ACCOUNT_DEBITED_FOR_TRANSFER,
  ACCOUNT_DEBITED_FOR_WITHDRAWAL,
  ACCOUNT_OPENED,
  TRANSFER_DECLINED_DUE_TO_INSUFFICIENT_FUNDS,
  WITHDRAWAL_DECLINED_DUE_TO_INSUFFICIENT_FUNDS,
} = require('../event.js')

function routeCommand (command) {
  const {data: {accountId}} = command

  return accountId
}

function createInitialState () {
  return 0
}

async function handleCommand (scope) {
  const {command: {type}} = scope

  switch (type) {
    case OPEN_ACCOUNT: return open(scope)
    case CREDIT_ACCOUNT_FOR_DEPOSIT: return creditForDeposit(scope)
    case CREDIT_ACCOUNT_FOR_TRANSFER: return creditForTransfer(scope)
    case DEBIT_ACCOUNT_FOR_WITHDRAWAL: return debitForWithdrawal(scope)
    case DEBIT_ACCOUNT_FOR_TRANSFER: return debitForTransfer(scope)
  }
}

async function open (scope) {
  const {command: {data: {accountId, name}}, recordEvents} = scope

  await recordEvents({type: ACCOUNT_OPENED, data: {accountId, name}})
}

async function creditForDeposit (scope) {
  const {command: {data: {accountId, amount, transactionId}}, recordEvents} = scope

  await recordEvents({type: ACCOUNT_CREDITED_FOR_DEPOSIT, data: {accountId, amount, transactionId}})
}

async function creditForTransfer (scope) {
  const {command: {data: {accountId, amount, transactionId}}, recordEvents} = scope

  await recordEvents({type: ACCOUNT_CREDITED_FOR_TRANSFER, data: {accountId, amount, transactionId}})
}

async function debitForWithdrawal (scope) {
  const {command: {data: {accountId, amount, transactionId}}, readState, recordEvents} = scope
  const balance = await readState()

  if (balance >= amount) {
    await recordEvents({type: ACCOUNT_DEBITED_FOR_WITHDRAWAL, data: {accountId, amount, transactionId}})
  } else {
    await recordEvents({type: WITHDRAWAL_DECLINED_DUE_TO_INSUFFICIENT_FUNDS, data: {accountId, amount, transactionId}})
  }
}

async function debitForTransfer (scope) {
  const {command: {data: {accountId, amount, transactionId}}, readState, recordEvents} = scope
  const balance = await readState()

  if (balance >= amount) {
    await recordEvents({type: ACCOUNT_DEBITED_FOR_TRANSFER, data: {accountId, amount, transactionId}})
  } else {
    await recordEvents({type: TRANSFER_DECLINED_DUE_TO_INSUFFICIENT_FUNDS, data: {accountId, amount, transactionId}})
  }
}

async function applyEvent (scope) {
  const {event: {type, data}, updateState} = scope

  switch (type) {
    case ACCOUNT_CREDITED_FOR_DEPOSIT:
    case ACCOUNT_CREDITED_FOR_TRANSFER: {
      const {amount} = data

      return updateState(balance => balance + amount)
    }

    case ACCOUNT_DEBITED_FOR_TRANSFER:
    case ACCOUNT_DEBITED_FOR_WITHDRAWAL: {
      const {amount} = data

      return updateState(balance => balance - amount)
    }
  }
}

module.exports = {
  commandTypes: [
    CREDIT_ACCOUNT_FOR_DEPOSIT,
    CREDIT_ACCOUNT_FOR_TRANSFER,
    DEBIT_ACCOUNT_FOR_TRANSFER,
    DEBIT_ACCOUNT_FOR_WITHDRAWAL,
    OPEN_ACCOUNT,
  ],
  eventTypes: [
    ACCOUNT_CREDITED_FOR_DEPOSIT,
    ACCOUNT_CREDITED_FOR_TRANSFER,
    ACCOUNT_DEBITED_FOR_TRANSFER,
    ACCOUNT_DEBITED_FOR_WITHDRAWAL,
    ACCOUNT_OPENED,
    TRANSFER_DECLINED_DUE_TO_INSUFFICIENT_FUNDS,
    WITHDRAWAL_DECLINED_DUE_TO_INSUFFICIENT_FUNDS,
  ],
  routeCommand,
  createInitialState,
  handleCommand,
  applyEvent,
}
