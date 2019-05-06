const {
  DEPOSIT,
  TRANSFER,
  WITHDRAW,
} = require('../command.js')

const {
  DEPOSIT_STARTED,
  TRANSFER_STARTED,
  WITHDRAWAL_STARTED,
} = require('../event.js')

function routeCommand (command) {
  const {data: {transactionId}} = command

  return transactionId
}

async function handleCommand (scope) {
  const {command: {type}} = scope

  switch (type) {
    case DEPOSIT: return deposit(scope)
    case TRANSFER: return transfer(scope)
    case WITHDRAW: return withdraw(scope)
  }
}

async function deposit (scope) {
  const {command: {data: {accountId, amount, transactionId}}, recordEvents} = scope

  await recordEvents({type: DEPOSIT_STARTED, data: {accountId, amount, transactionId}})
}

async function transfer (scope) {
  const {command: {data: {accountId, amount, transactionId}}, recordEvents} = scope

  await recordEvents({type: TRANSFER_STARTED, data: {accountId, amount, transactionId}})
}

async function withdraw (scope) {
  const {command: {data: {amount, fromAccountId, toAccountId, transactionId}}, recordEvents} = scope

  await recordEvents({type: WITHDRAWAL_STARTED, data: {amount, fromAccountId, toAccountId, transactionId}})
}

module.exports = {
  commandTypes: [
    DEPOSIT,
    TRANSFER,
    WITHDRAW,
  ],
  eventTypes: [
    DEPOSIT_STARTED,
    TRANSFER_STARTED,
    WITHDRAWAL_STARTED,
  ],
  routeCommand,
  handleCommand,
}
