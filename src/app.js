const {createLogger} = require('./logging.js')
const {createPool} = require('./pg.js')
const {initializeSchema} = require('./schema.js')
const {maintainProjection} = require('./projection.js')
const {normalizeSpec} = require('./spec.js')
const {serialization: jsonSerialization} = require('./serialization/json.js')
const {systemClock} = require('./clock.js')

module.exports = {
  createApp,
}

function createApp (spec) {
  const normalizedSpec = normalizeSpec(spec)
  const {name} = normalizedSpec

  return {
    name,
    run: createRun(normalizedSpec),
  }
}

function createRun (spec) {
  const {initialize, name, projections} = spec

  return async function run (options = {}) {
    const {env = process.env} = options

    const {
      clock = systemClock,
      logger = createLogger(env),
      pgConfig = {},
      serialization = jsonSerialization,
    } = options

    logger.info(`Running ${name}`)

    const pgPool = createPool(pgConfig)
    const pgClient = await pgPool.connect()

    try {
      await initializeSchema(pgClient)
      await initialize(pgClient)
    } finally {
      pgClient.release()
    }

    const projectionThreads = Object.entries(projections).map(([name, projection]) => {
      const {applyEvent} = projection

      return [
        `projection.${name}`,
        maintainProjection(serialization, pgPool, name, applyEvent, {clock}),
      ]
    })

    const threads = [
      ...projectionThreads,
    ]

    const runners = threads.map(async ([type, thread]) => {
      logger.debug(`Waiting for ${type}`)

      for await (const result of thread) {
        logger.debug(
          typeof result === 'undefined'
            ? logger.debug(`Iterated ${type}`)
            : logger.debug(`Iterated ${type}: ${JSON.stringify(result)}`)
        )
      }
    })

    Promise.all(runners).then(
      () => { logger.info('Stopping') },
      error => { logger.error(`Stopping: ${error.stack}`) }
    )

    return function stop () {
      const cancellations = threads.map(([_, thread]) => thread.cancel())

      Promise.all(cancellations)
        .catch(error => { logger.error(`Cancellation failed: ${error.stack}`) })
    }
  }
}
