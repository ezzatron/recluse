const {handleCommandWithAggregate} = require('./aggregate.js')
const {readUnhandledCommandsContinuously} = require('./command.js')
const {handleCommandWithIntegration} = require('./integration.js')
const {COMMAND: LOCK_NAMESPACE} = require('./lock.js')
const {inPoolTransaction, query, withAdvisoryLock, withClient} = require('./pg.js')

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

  const mapping = createTypeMapping(logger, aggregates, integrations)

  return async function handleCommand (context, client, command) {
    const {type} = command
    const mapped = mapping[type]

    if (!mapped) throw new Error(`Unable to handle ${type} command - no suitable handler found`)

    const {handler, name} = mapped

    if (mapped.handlerType === HANDLER_TYPE_INTEGRATION) {
      await handleCommandWithIntegration(context, logger, client, serialization, name, handler, command)

      return true
    }

    return handleCommandWithAggregate(context, logger, client, serialization, name, handler, command)
  }
}

async function maintainCommandHandler (context, logger, pool, serialization, handleCommand, options = {}) {
  const {shouldContinue, timeout} = options

  logger.debug('Acquiring client for command handling')

  await withClient(context, logger, pool, async client => {
    logger.debug('Acquired client for command handling')
    logger.debug('Acquiring session lock for command handling')

    await withAdvisoryLock(context, logger, client, LOCK_NAMESPACE, 0, async () => {
      logger.debug('Acquired session lock for command handling')

      await readUnhandledCommandsContinuously(context, logger, client, serialization, {timeout}, async wrapper => {
        await consumeCommand(context, logger, pool, handleCommand, wrapper)

        if (shouldContinue && !shouldContinue()) return false

        logger.debug('Awaiting command')

        return true
      })
    })
  })
}

async function consumeCommand (context, logger, pool, handleCommand, wrapper) {
  const {command} = wrapper
  const {type} = command
  logger.debug(`Consuming ${type} command`)

  await inPoolTransaction(context, logger, pool, async client => {
    logger.debug(`Handling ${type} command`)
    const isHandled = await handleCommand(context, client, command)

    if (!isHandled) throw new Error(`Unable to handle ${type} command`)

    logger.debug(`Recording ${type} command as handled`)
    await commandHandled(context, logger, client, wrapper)

    logger.info(`Consumed ${type} command`)
  })
}

async function commandHandled (context, logger, client, wrapper) {
  const {id} = wrapper

  return query(
    context,
    logger,
    client,
    'UPDATE recluse.command SET handled_at = now() WHERE id = $1',
    [id],
  )
}

function createTypeMapping (logger, aggregates, integrations) {
  const index = {}

  addTypeMappingEntries(logger, index, HANDLER_TYPE_AGGREGATE, aggregates)
  addTypeMappingEntries(logger, index, HANDLER_TYPE_INTEGRATION, integrations)

  return index
}

function addTypeMappingEntries (logger, index, handlerType, handlers) {
  for (const name in handlers) {
    const handler = handlers[name]

    for (const commandType of handler.commandTypes) {
      addTypeMappingEntry(logger, index, commandType, {handler, handlerType, name})
    }
  }
}

function addTypeMappingEntry (logger, index, commandType, entry) {
  if (!index[commandType]) {
    index[commandType] = entry

    return
  }

  const {handlerType: existingHandlerType, name: existingName} = index[commandType]
  const {handlerType: entryHandlerType, name: entryName} = entry

  throw new Error(
    `Commands of type ${commandType} are already handled by the ${existingName} ${existingHandlerType}, ` +
    `and cannot be handled by the ${entryName} ${entryHandlerType}`,
  )
}
