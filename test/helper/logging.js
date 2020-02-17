const winston = require('winston')
const {NullTransport} = require('winston-null')

module.exports = {
  createLogger,
}

function createLogger () {
  return winston.createLogger({transports: [new NullTransport()]})
}
