const {
  CREDIT_ACCOUNT_FOR_DEPOSIT,
} = require('../command.js')

const {
  DEPOSIT_STARTED,
} = require('../event.js')

function routeEvent (event) {
  const {data: {transactionId}} = event

  return transactionId
}

function handleEvent (scope) {
  const {event: {type}} = scope

  switch (type) {
    case DEPOSIT_STARTED: return credit(scope)
  }
}

function credit (scope) {
  const {event: {data: {accountId, amount, transactionId}}, executeCommands} = scope

  executeCommands({type: CREDIT_ACCOUNT_FOR_DEPOSIT, data: {accountId, amount, transactionId}})
}

module.exports = {
  eventTypes: [
    DEPOSIT_STARTED,
  ],
  commandTypes: [
    CREDIT_ACCOUNT_FOR_DEPOSIT,
  ],
  routeEvent,
  handleEvent,
}
