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

function createCommandHandler (serialization, aggregates, integrations) {
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
      return handleCommandWithIntegration(serialization, pgClient, name, handler, command)
    }

    return handleCommandWithAggregate(serialization, pgClient, name, handler, command)
  }
}

function maintainCommandHandler (serialization, pgPool, handleCommand, options = {}) {
  const {timeout, clock} = options
  const iterator = createCommandIterator(serialization, pgPool, handleCommand, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

function createCommandIterator (serialization, pgPool, handleCommand, timeout, clock) {
  let iterator, pgClient
  let isLocked = false

  return {
    async next () {
      if (!iterator) {
        pgClient = await pgPool.connect()

        await acquireSessionLock(pgClient, LOCK_NAMESPACE)
        isLocked = true

        iterator = acquireAsyncIterator(readUnhandledCommandsContinuously(serialization, pgClient, {timeout, clock}))
      }

      const {value: wrapper} = await iterator.next()
      await consumeCommand(pgPool, handleCommand, wrapper)

      return {done: false}
    },

    async cancel () {
      const errors = []

      if (iterator) {
        try {
          await iterator.cancel()
        } catch (error) {
          errors.push(error)
        }
      }

      if (isLocked) {
        try {
          await releaseSessionLock(pgClient, LOCK_NAMESPACE)
        } catch (error) {
          errors.push(error)
        }
      }

      if (pgClient) {
        try {
          pgClient.release()
        } catch (error) {
          errors.push(error)
        }
      }

      if (errors.length > 0) throw errors[0]
    },
  }
}

async function consumeCommand (pgPool, handleCommand, wrapper) {
  const {command} = wrapper
  const {type} = command

  const pgClient = await pgPool.connect()

  try {
    await inTransaction(pgClient, async () => {
      const isHandled = await handleCommand(pgClient, command)

      if (!isHandled) throw new Error(`Unable to handle ${type} command`)

      await commandHandled(pgClient, wrapper)
    })
  } finally {
    pgClient.release()
  }
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
