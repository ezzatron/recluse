const winston = require('winston')

const DEFAULT_LOG_LEVEL = 'info'

module.exports = {
  createLoggerFactory,
}

function createLoggerFactory (env) {
  const {LOG_LEVEL: level = DEFAULT_LOG_LEVEL} = env
  const runnerId = Math.random().toString(36).substring(2, 15)
  const container = new winston.Container()

  return function createLogger (label) {
    if (!container.has(label)) {
      container.add(label, {
        format: winston.format.combine(
          winston.format.colorize(),
          winston.format.timestamp(),
          winston.format.label({label}),
          winston.format.printf(({label, level, message, timestamp}) => {
            return `[${timestamp}] [${runnerId}] [${label}] [${level}] ${message}`
          }),
        ),
        transports: [new winston.transports.Console({level})],
      })
    }

    return container.get(label)
  }
}
