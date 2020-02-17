const {
  CREDIT_ACCOUNT_FOR_DEPOSIT,
} = require('../command.js')

const {
  DEPOSIT_STARTED,
} = require('../event.js')

module.exports = {
  eventTypes: [
    DEPOSIT_STARTED,
  ],

  commandTypes: [
    CREDIT_ACCOUNT_FOR_DEPOSIT,
  ],

  routeEvent (event) {
    const {data: {transactionId}} = event

    return transactionId
  },

  handleEvent (scope) {
    const {event: {type}} = scope

    switch (type) {
      case DEPOSIT_STARTED: return credit(scope)
    }
  },
}

function credit (scope) {
  const {event: {data: {accountId, amount, transactionId}}, executeCommands} = scope

  executeCommands({type: CREDIT_ACCOUNT_FOR_DEPOSIT, data: {accountId, amount, transactionId}})
}
