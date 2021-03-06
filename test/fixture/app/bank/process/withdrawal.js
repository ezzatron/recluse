const {
  DEBIT_ACCOUNT_FOR_WITHDRAWAL,
} = require('../command.js')

const {
  WITHDRAWAL_STARTED,
} = require('../event.js')

module.exports = {
  eventTypes: [
    WITHDRAWAL_STARTED,
  ],

  commandTypes: [
    DEBIT_ACCOUNT_FOR_WITHDRAWAL,
  ],

  routeEvent (event) {
    const {data: {transactionId}} = event

    return transactionId
  },

  handleEvent (scope) {
    const {event: {type}} = scope

    switch (type) {
      case WITHDRAWAL_STARTED: return debit(scope)
    }
  },
}

function debit (scope) {
  const {event: {data: {accountId, amount, transactionId}}, executeCommands} = scope

  executeCommands({type: DEBIT_ACCOUNT_FOR_WITHDRAWAL, data: {accountId, amount, transactionId}})
}
