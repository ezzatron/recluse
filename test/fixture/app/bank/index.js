const account = require('./aggregate/account.js')
const transaction = require('./aggregate/transaction.js')
const createInitialize = require('./initialize.js')
const deposit = require('./process/deposit.js')
const transfer = require('./process/transfer.js')
const withdrawal = require('./process/withdrawal.js')
const createRelationalProjection = require('./projection/relational.js')

module.exports = function createSpec (schemaName) {
  return {
    name: 'bank',

    initialize: createInitialize(schemaName),

    aggregates: {
      account,
      transaction,
    },
    processes: {
      deposit,
      transfer,
      withdrawal,
    },
    projections: {
      relational: createRelationalProjection(schemaName),
    },
  }
}
