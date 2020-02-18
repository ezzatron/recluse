const {systemClock} = require('./clock.js')
const {createCommandHandler, maintainCommandHandler} = require('./command-handler.js')
const {createLoggerFactory} = require('./logging.js')
const {createPool} = require('./pg.js')
const {maintainProcess} = require('./process.js')
const {maintainProjection} = require('./projection.js')
const {initializeSchema} = require('./schema.js')
const {serialization: jsonSerialization} = require('./serialization/json.js')
const {normalizeSpec} = require('./spec.js')

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
  const {
    aggregates,
    initialize,
    integrations,
    name,
    processes,
    projections,
  } = spec

  return async function run (options = {}) {
    const {
      env = process.env,
      exit = process.exit,
    } = options

    const {
      clock = systemClock,
      createLogger = createLoggerFactory(env),
      pgConfig = {},
      serialization = jsonSerialization,
    } = options

    const logger = createLogger('main')

    logger.info(`Running ${name}`)

    const pgPool = createPool(pgConfig)
    const pgClient = await pgPool.connect()

    try {
      await initializeSchema(pgClient)
      await initialize(pgClient)
    } finally {
      pgClient.release()
    }

    const commandHandlerThreadType = 'command-handler'
    const commandHandlerLogger = createLogger(commandHandlerThreadType)
    const commandHandlerThread = [
      commandHandlerThreadType,
      maintainCommandHandler(
        commandHandlerLogger,
        serialization,
        pgPool,
        createCommandHandler(commandHandlerLogger, serialization, aggregates, integrations),
        {clock},
      ),
    ]

    const processThreads = Object.entries(processes).map(([name, process]) => {
      const type = `process.${name}`

      return [
        type,
        maintainProcess(
          createLogger(type),
          serialization,
          pgPool,
          name,
          process,
          {clock},
        ),
      ]
    })

    const projectionThreads = Object.entries(projections).map(([name, projection]) => {
      const type = `projection.${name}`

      return [
        type,
        maintainProjection(
          createLogger(type),
          serialization,
          pgPool,
          name,
          projection,
          {clock},
        ),
      ]
    })

    const threads = [
      commandHandlerThread,
      ...processThreads,
      ...projectionThreads,
    ]

    const runners = threads.map(async ([type, thread]) => {
      logger.debug(`Waiting for ${type}`)

      for await (const result of thread) {
        logger.debug(
          typeof result === 'undefined'
            ? `Iterated ${type}`
            : `Iterated ${type}: ${JSON.stringify(result)}`,
        )
      }
    })

    function die (code, message) {
      if (code === 0) {
        logger.info(message)
      } else {
        logger.error(message)
      }

      exit(code)
    }

    Promise.all(runners).then(
      () => die(0, `Stopping ${name}`),
      error => die(1, `Stopping ${name}: ${error.stack}`),
    )

    return async function stop () {
      const cancellations = threads.map(([, thread]) => thread.cancel())

      await Promise.all(cancellations)
        .catch(error => die(1, `Cancellation failed: ${error.stack}`))
    }
  }
}
