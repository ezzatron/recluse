const {handleCommandWithAggregate} = require('./aggregate.js')
const {readUnhandledCommandsContinuously} = require('./command.js')
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

function maintainCommandHandler (logger, serialization, pgPool, handleCommand, options = {}) {
  const {timeout, clock} = options
  const iterator = createCommandIterator(logger, serialization, pgPool, handleCommand, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

function createCommandIterator (logger, serialization, pgPool, handleCommand, timeout, clock) {
  let iterator, pgClient
  let isLocked = false

  return {
    async next () {
      if (!iterator) {
        logger.debug('Acquiring Postgres client for command iteration')
        pgClient = await pgPool.connect()

        logger.debug('Acquiring session lock for command handling')
        await acquireSessionLock(pgClient, LOCK_NAMESPACE)
        isLocked = true
        logger.debug('Acquired session lock for command handling')

        logger.debug('Creating command iterator')
        iterator = acquireAsyncIterator(
          readUnhandledCommandsContinuously(logger, serialization, pgClient, {timeout, clock}),
        )
      }

      logger.debug('Awaiting command')
      const {done, value: wrapper} = await iterator.next()

      if (done) {
        logger.debug('Command iterator ended, stopping command handler')

        return {done: true}
      }

      await consumeCommand(logger, pgPool, handleCommand, wrapper)

      return {done: false}
    },

    async cancel () {
      const errors = []

      logger.debug('Cancelling command handler')

      if (iterator) {
        logger.debug('Cancelling command iterator')

        try {
          await iterator.cancel()
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to cancel command iterator: ${error.stack}`)
        }
      } else {
        logger.debug('No command iterator to cancel')
      }

      if (isLocked) {
        logger.debug('Releasing session lock for command handling')

        try {
          await releaseSessionLock(pgClient, LOCK_NAMESPACE)
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to release session lock for command handling: ${error.stack}`)
        }
      } else {
        logger.debug('The session lock for command handling was never acquired')
      }

      if (pgClient) {
        logger.debug('Releasing Postgres client for command iteration')

        try {
          pgClient.release()
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to release Postgres client for command iteration: ${error.stack}`)
        }
      } else {
        logger.debug('A Postgres client for command iteration was never acquired')
      }

      if (errors.length > 0) throw errors[0]

      logger.debug('Cancelled command handler')
    },
  }
}

async function consumeCommand (logger, pgPool, handleCommand, wrapper) {
  const {command} = wrapper
  const {type} = command

  logger.debug(`Consuming ${type} command`)

  logger.debug(`Acquiring Postgres client to handle ${type} command`)
  const pgClient = await pgPool.connect()

  try {
    await inTransaction(pgClient, async () => {
      logger.debug(`Handling ${type} command`)
      const isHandled = await handleCommand(pgClient, command)

      if (!isHandled) throw new Error(`Unable to handle ${type} command`)

      logger.debug(`Recording ${type} command as handled`)
      await commandHandled(pgClient, wrapper)
    })
  } finally {
    logger.debug(`Releasing Postgres client used to handle ${type} command`)
    pgClient.release()
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
