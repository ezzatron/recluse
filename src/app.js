const {systemClock} = require('./clock.js')
const {createCommandHandler, maintainCommandHandler} = require('./command-handler.js')
const {createLogger} = require('./logging.js')
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

    const maintainOptions = {clock}

    const commandHandlerThread = [
      'command-handler',
      maintainCommandHandler(
        serialization,
        pgPool,
        createCommandHandler(serialization, aggregates, integrations),
        maintainOptions,
      ),
    ]

    const processThreads = Object.entries(processes).map(([name, process]) => {
      return [
        `process.${name}`,
        maintainProcess(serialization, pgPool, name, process, maintainOptions),
      ]
    })

    const projectionThreads = Object.entries(projections).map(([name, projection]) => {
      return [
        `projection.${name}`,
        maintainProjection(serialization, pgPool, name, projection, maintainOptions),
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
      () => die(0, 'Stopping'),
      error => die(1, `Stopping: ${error.stack}`),
    )

    return function stop () {
      const cancellations = threads.map(([, thread]) => thread.cancel())

      Promise.all(cancellations)
        .catch(error => die(1, `Cancellation failed: ${error.stack}`))
    }
  }
}
