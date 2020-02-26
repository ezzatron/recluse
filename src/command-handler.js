const {handleCommandWithAggregate} = require('./aggregate.js')
const {readUnhandledCommandsContinuously} = require('./command.js')
const {Canceled, createContext} = require('./context.js')
const {handleCommandWithIntegration} = require('./integration.js')
const {acquireAsyncIterator} = require('./iterator.js')
const {COMMAND: LOCK_NAMESPACE} = require('./lock.js')
const {acquireSessionLock, inTransaction, releaseSessionLock} = require('./pg.js')

module.exports = {
  createCommandHandler,
  maintainCommandHandler,
}

const HANDLER_TYPE_AGGREGATE = 'aggregate'
const HANDLER_TYPE_INTEGRATION = 'integration'

function createCommandHandler (logger, serialization, aggregates, integrations) {
  if (!logger) throw new Error('Invalid logger')
  if (!serialization) throw new Error('Invalid serialization')
  if (!aggregates) throw new Error('Invalid aggregates')
  if (!integrations) throw new Error('Invalid integrations')

  const mapping = createTypeMapping(aggregates, integrations)

  return async function handleCommand (pgClient, command) {
    const {type} = command
    const mapped = mapping[type]

    if (!mapped) throw new Error(`Unable to handle ${type} command - no suitable handler found`)

    const {handler, name} = mapped

    if (mapped.handlerType === HANDLER_TYPE_INTEGRATION) {
      return handleCommandWithIntegration(logger, serialization, pgClient, name, handler, command)
    }

    return handleCommandWithAggregate(logger, serialization, pgClient, name, handler, command)
  }
}

function maintainCommandHandler (context, logger, serialization, pgPool, handleCommand, options = {}) {
  const {timeout, clock} = options
  const iterator = createCommandIterator(context, logger, serialization, pgPool, handleCommand, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
  }
}

function createCommandIterator (context, logger, serialization, pgPool, handleCommand, timeout, clock) {
  let iterator, pgClient

  return {
    async next () {
      try {
        if (!iterator) {
          pgClient = await context.do(async () => {
            logger.debug('Acquiring Postgres client for command iteration')

            const pgClient = await pgPool.connect()
            await context.onceDone(() => pgClient.release())

            return pgClient
          })

          await context.do(async () => {
            logger.debug('Acquiring session lock for command handling')
            await acquireSessionLock(pgClient, LOCK_NAMESPACE)
            logger.debug('Acquired session lock for command handling')

            await context.onceDone(() => releaseSessionLock(pgClient, LOCK_NAMESPACE))
          })

          logger.debug('Creating command iterator')
          iterator = acquireAsyncIterator(
            readUnhandledCommandsContinuously(context, logger, serialization, pgClient, {timeout, clock}),
          )
        }

        logger.debug('Awaiting command')
        const {done, value: wrapper} = await iterator.next()

        if (done) {
          logger.debug('Command iterator ended, stopping command handler')

          return {done: true}
        }

        await consumeCommand(context, logger, pgPool, handleCommand, wrapper)

        return {done: false}
      } catch (error) {
        if (error instanceof Canceled) {
          logger.debug('Detected cancellation of command handling')

          return {done: true}
        }

        throw error
      }
    },
  }
}

async function consumeCommand (context, logger, pgPool, handleCommand, wrapper) {
  const {command} = wrapper
  const {type} = command

  logger.debug(`Consuming ${type} command`)

  const consumeContext = createContext({context})

  try {
    const pgClient = await consumeContext.do(async () => {
      logger.debug(`Acquiring Postgres client to handle ${type} command`)

      const pgClient = await pgPool.connect()
      await consumeContext.onceDone(() => pgClient.release())

      return pgClient
    })

    await inTransaction(pgClient, async () => {
      logger.debug(`Handling ${type} command`)
      const isHandled = await handleCommand(pgClient, command)

      if (!isHandled) throw new Error(`Unable to handle ${type} command`)

      logger.debug(`Recording ${type} command as handled`)
      await commandHandled(pgClient, wrapper)
    })
  } finally {
    await consumeContext.cancel()
  }

  logger.debug(`Consumed ${type} command`)
}

async function commandHandled (pgClient, wrapper) {
  const {id} = wrapper

  return pgClient.query(
    'UPDATE recluse.command SET handled_at = now() WHERE id = $1',
    [id],
  )
}

function createTypeMapping (aggregates, integrations) {
  const index = {}

  addTypeMappingEntries(index, HANDLER_TYPE_AGGREGATE, aggregates)
  addTypeMappingEntries(index, HANDLER_TYPE_INTEGRATION, integrations)

  return index
}

function addTypeMappingEntries (index, handlerType, handlers) {
  for (const name in handlers) {
    const handler = handlers[name]

    handler.commandTypes.forEach(type => addTypeMappingEntry(index, type, {handler, handlerType, name}))
  }
}

function addTypeMappingEntry (index, type, entry) {
  if (!index[type]) {
    index[type] = entry

    return
  }

  const {handlerType: existingHandlerType, name: existingName} = index[type]
  const {handlerType: entryHandlerType, name: entryName} = entry

  throw new Error(
    `Commands of type ${type} are already handled by the ${existingName} ${existingHandlerType}, ` +
    `and cannot be handled by the ${entryName} ${entryHandlerType}`,
  )
}
