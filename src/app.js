const {createContext, isCanceled} = require('./async.js')
const {createCommandHandler, maintainCommandHandler} = require('./command-handler.js')
const {createLoggerFactory} = require('./logging.js')
const {createPool, inPoolTransaction} = require('./pg.js')
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
    } = options

    const {
      context: parentContext,
      createLogger = createLoggerFactory(env),
      exit = process.exit,
      pgConfig: userPgConfig,
      serialization = jsonSerialization,
    } = options

    const logger = createLogger('main')
    const [context, cancel] = createContext(logger, {context: parentContext})

    logger.info(`Running ${name}`)

    const pgConfig = resolvePgConfig(spec, userPgConfig)
    const pool = createPool(pgConfig)
    await initializeSchema(context, logger, pool)

    await inPoolTransaction(context, logger, pool, async client => {
      await initialize(client)
    })

    const commandHandlerThreadType = 'command-handler'
    const handleCommand = createCommandHandler(logger, serialization, aggregates, integrations)
    const commandHandlerThread =
      maintainCommandHandler(context, createLogger(commandHandlerThreadType), pool, serialization, handleCommand)

    const processThreads = []

    for (const [name, process] of Object.entries(processes)) {
      processThreads.push(maintainProcess(
        context,
        createLogger(`process.${name}`),
        pool,
        serialization,
        name,
        process,
      ))
    }

    const projectionThreads = []

    for (const [name, projection] of Object.entries(projections)) {
      projectionThreads.push(maintainProjection(
        context,
        createLogger(`projection.${name}`),
        pool,
        serialization,
        name,
        projection,
      ))
    }

    const threads = [
      commandHandlerThread,
      ...processThreads,
      ...projectionThreads,
    ]

    Promise.all(threads).catch(error => {
      if (isCanceled(error)) {
        die(0, `Stopping ${name}`)
      } else {
        die(1, `Stopping ${name}: ${error.stack}`)
      }
    })

    return cancel

    function die (code, message) {
      if (code === 0) {
        logger.info(message)
      } else {
        logger.error(message)
      }

      exit(code)
    }
  }
}

function resolvePgConfig (spec, config = {}) {
  const {
    connectionTimeoutMillis = 1000,
    idleTimeoutMillis = 0,
    max = defaultPoolSize(spec),
  } = config

  return {
    ...config,
    connectionTimeoutMillis,
    idleTimeoutMillis,
    max,
  }
}

function defaultPoolSize (spec) {
  const {processes, projections} = spec
  const threadCount = 1 + Object.keys(processes).length + Object.keys(projections).length

  return 10 + threadCount
}
