module.exports = {
  name: 'bank',

  initialize: require('./initialize.js'),

  aggregates: require('./aggregate/index.js'),
  processes: require('./process/index.js'),
  projections: require('./projection/index.js'),
}
