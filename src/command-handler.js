const {handleCommandWithAggregate} = require('./aggregate.js')
const {handleCommandWithIntegration} = require('./integration.js')

module.exports = {
  createCommandHandler,
}

const HANDLER_TYPE_AGGREGATE = 'aggregate'
const HANDLER_TYPE_INTEGRATION = 'integration'

function createCommandHandler (logger, serialization, aggregates, integrations) {
  if (!logger) throw new Error('Invalid logger')
  if (!serialization) throw new Error('Invalid serialization')
  if (!aggregates) throw new Error('Invalid aggregates')
  if (!integrations) throw new Error('Invalid integrations')

  const mapping = createTypeMapping(aggregates, integrations)

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

function createTypeMapping (aggregates, integrations) {
  const index = {}

  addTypeMappingEntries(index, HANDLER_TYPE_AGGREGATE, aggregates)
  addTypeMappingEntries(index, HANDLER_TYPE_INTEGRATION, integrations)

  return index
}

function addTypeMappingEntries (index, handlerType, handlers) {
  for (const name in handlers) {
    const handler = handlers[name]

    for (const commandType of handler.commandTypes) {
      addTypeMappingEntry(index, commandType, {handler, handlerType, name})
    }
  }
}

function addTypeMappingEntry (index, commandType, entry) {
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
