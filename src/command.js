const {acquireAsyncIterator} = require('./iterator.js')
const {acquireSessionLock, asyncQuery, continuousQuery, inTransaction, releaseSessionLock} = require('./pg.js')
const {allSerial} = require('./async.js')
const {appendEvents, appendEventsUnchecked, readEventsByStream} = require('./event.js')
const {COMMAND: CHANNEL} = require('./channel.js')
const {COMMAND: LOCK_NAMESPACE} = require('./lock.js')

module.exports = {
  createCommandHandler,
  executeCommands,
  maintainCommandHandler,
  readCommands,
  readUnhandledCommandsContinuously,
}

const HANDLER_TYPE_AGGREGATE = 'aggregate'
const HANDLER_TYPE_INTEGRATION = 'integration'

function createCommandHandler (aggregates, integrations) {
  if (!aggregates) throw new Error('Invalid aggregates')
  if (!integrations) throw new Error('Invalid integrations')

  const mapping = createTypeMapping(aggregates, integrations)

  return async function handleCommand (pgClient, command) {
    const {type} = command
    const mapped = mapping[type]

    if (!mapped) throw new Error(`Unable to handle ${type} command - no suitable handler found`)

    const {handler, name} = mapped

    if (mapped.handlerType === HANDLER_TYPE_INTEGRATION) {
      return handleCommandWithIntegration(pgClient, name, handler, command)
    }

    return handleCommandWithAggregate(pgClient, name, handler, command)
  }
}

async function executeCommands (pgClient, source, commands) {
  for (const command of commands) {
    const {type, data = null} = command

    await pgClient.query(
      'INSERT INTO recluse.command (type, data, source) VALUES ($1, $2, $3)',
      [type, data, source]
    )
  }

  await pgClient.query(`NOTIFY ${CHANNEL}`)
}

function maintainCommandHandler (pgPool, handleCommand, options = {}) {
  const {timeout, clock} = options
  const iterator = createCommandIterator(pgPool, handleCommand, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

function createCommandIterator (pgPool, handleCommand, timeout, clock) {
  let iterator, pgClient
  let isLocked = false

  return {
    async next () {
      if (!iterator) {
        pgClient = await pgPool.connect()

        await acquireSessionLock(pgClient, LOCK_NAMESPACE)
        isLocked = true

        iterator = acquireAsyncIterator(readUnhandledCommandsContinuously(pgClient, {timeout, clock}))
      }

      const {value: wrapper} = await iterator.next()
      await consumeCommand(pgPool, handleCommand, wrapper)

      return {done: false}
    },

    async cancel () {
      await allSerial(
        async () => { if (iterator) await iterator.cancel() },
        async () => { if (isLocked) await releaseSessionLock(pgClient, LOCK_NAMESPACE) },
        () => { if (pgClient) pgClient.release() },
      )
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
    [id]
  )
}

function readCommands (pgClient, id = 0) {
  return pgClient.query(asyncQuery(
    'SELECT * FROM recluse.command WHERE id >= $1 ORDER BY id',
    [id],
    marshal
  ))
}

function readUnhandledCommandsContinuously (pgClient, options = {}) {
  const {clock, timeout} = options

  return continuousQuery(
    pgClient,
    'SELECT * FROM recluse.command WHERE handled_at IS NULL AND id >= $1 ORDER BY id',
    CHANNEL,
    ({id}) => id + 1,
    {clock, marshal, timeout}
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
    `and cannot be handled by the ${entryName} ${entryHandlerType}`
  )
}

async function handleCommandWithAggregate (pgClient, name, aggregate, command) {
  const {applyEvent, createInitialState, eventTypes, handleCommand, routeCommand} = aggregate
  const {type} = command
  const instance = routeCommand(command)

  if (!instance) throw new Error(`Unable to handle ${type} command - no suitable route found`)

  const streamType = `aggregate.${name}`
  const {state, next} = await readAggregateState(pgClient, streamType, instance, applyEvent, createInitialState())
  const recordedEvents = []

  function recordEvents (...events) {
    events.forEach(event => {
      const {type} = event

      if (!eventTypes.includes(type)) throw new Error(`Aggregate ${name} cannot record ${type} events`)

      recordedEvents.push(event)
      applyEvent(state, event)
    })
  }

  await handleCommand({command, recordEvents, state})

  return appendEvents(pgClient, streamType, instance, next, recordedEvents)
}

async function handleCommandWithIntegration (pgClient, name, integration, command) {
  const {eventTypes, handleCommand} = integration

  const streamType = `integration.${name}`
  const recordedEvents = []

  function recordEvents (...events) {
    events.forEach(event => {
      const {type} = event

      if (!eventTypes.includes(type)) throw new Error(`Integration ${name} cannot record ${type} events`)

      recordedEvents.push(event)
    })
  }

  await handleCommand({command, recordEvents})

  return appendEventsUnchecked(pgClient, streamType, '', recordedEvents)
}

async function readAggregateState (pgClient, streamType, instance, applyEvent, state) {
  const events = readEventsByStream(pgClient, streamType, instance)
  let next = 0

  for await (const wrapper of events) {
    const {event, streamOffset} = wrapper

    applyEvent(state, event)
    next = parseInt(streamOffset) + 1
  }

  return {state, next}
}

function marshal (row) {
  const {
    data,
    executed_at: executedAt,
    handled_at: handledAt,
    id,
    source,
    type,
  } = row

  return {
    command: data === null ? {type} : {type, data},
    executedAt,
    handledAt,
    id: parseInt(id),
    source,
  }
}
