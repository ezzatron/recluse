const winston = require('winston')
const {NullTransport} = require('winston-null')

const {createLoggerFactory} = require('../../src/logging.js')

module.exports = {
  createLogger,
}

function createLogger () {
  const {env} = process
  const {TEST_LOG_LEVEL} = env

  if (TEST_LOG_LEVEL) {
    const createLogger = createLoggerFactory({...env, LOG_LEVEL: TEST_LOG_LEVEL})

    return createLogger('test')
  }

  return winston.createLogger({transports: [new NullTransport()]})
}
