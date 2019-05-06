const {
  CREDIT_ACCOUNT_FOR_TRANSFER,
  DEBIT_ACCOUNT_FOR_TRANSFER,
} = require('../command.js')

const {
  ACCOUNT_DEBITED_FOR_TRANSFER,
  TRANSFER_STARTED,
} = require('../event.js')

function routeEvent (event) {
  const {data: {transactionId}} = event

  return transactionId
}

async function handleEvent (scope) {
  const {event: {type}} = scope

  switch (type) {
    case TRANSFER_STARTED: return debit(scope)
    case ACCOUNT_DEBITED_FOR_TRANSFER: return credit(scope)
  }
}

async function debit (scope) {
  const {event: {data: {amount, fromAccountId, toAccountId, transactionId}}, executeCommands, updateState} = scope

  await updateState(toAccountId)
  executeCommands({type: DEBIT_ACCOUNT_FOR_TRANSFER, data: {accountId: fromAccountId, amount, transactionId}})
}

async function credit (scope) {
  const {event: {data: {amount, transactionId}}, executeCommands, readState} = scope
  const accountId = await readState()

  executeCommands({type: CREDIT_ACCOUNT_FOR_TRANSFER, data: {accountId, amount, transactionId}})
}

module.exports = {
  eventTypes: [
    ACCOUNT_DEBITED_FOR_TRANSFER,
    TRANSFER_STARTED,
  ],
  commandTypes: [
    CREDIT_ACCOUNT_FOR_TRANSFER,
    DEBIT_ACCOUNT_FOR_TRANSFER,
  ],
  routeEvent,
  handleEvent,
}
