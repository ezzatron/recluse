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

function handleCommand (scope) {
  const {command: {type}} = scope

  switch (type) {
    case DEPOSIT: return deposit(scope)
    case TRANSFER: return transfer(scope)
    case WITHDRAW: return withdraw(scope)
  }
}

function deposit (scope) {
  const {command: {accountId, amount, transactionId}, recordEvents} = scope

  recordEvents({type: DEPOSIT_STARTED, data: {accountId, amount, transactionId}})
}

function transfer (scope) {
  const {command: {accountId, amount, transactionId}, recordEvents} = scope

  recordEvents({type: TRANSFER_STARTED, data: {accountId, amount, transactionId}})
}

function withdraw (scope) {
  const {command: {amount, fromAccountId, toAccountId, transactionId}, recordEvents} = scope

  recordEvents({type: WITHDRAWAL_STARTED, data: {amount, fromAccountId, toAccountId, transactionId}})
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
