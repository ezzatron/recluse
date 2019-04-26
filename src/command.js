const {acquireAsyncIterator} = require('./iterator.js')
const {acquireSessionLock, asyncQuery, continuousQuery, inTransaction, releaseSessionLock} = require('./pg.js')
const {allSerial} = require('./async.js')
const {appendEvents, readEventsByStream} = require('./event.js')
const {COMMAND: CHANNEL} = require('./channel.js')
const {COMMAND: LOCK_NAMESPACE} = require('./lock.js')

module.exports = {
  createCommandHandler,
  executeCommands,
  maintainCommandHandler,
  readCommands,
  readUnhandledCommandsContinuously,
}

function createCommandHandler (aggregates) {
  const mapping = createTypeMapping(aggregates)

  return async function handleCommand (pgClient, command) {
    const {type} = command
    const mapped = mapping[type]

    if (!mapped) throw new Error(`Unable to handle ${type} command - no suitable aggregates found`)

    const {aggregate, name} = mapped
    const {applyEvent, createInitialState, eventTypes, handleCommand, routeCommand} = aggregate
    const stream = routeCommand(command)

    if (!stream) throw new Error(`Unable to handle ${type} command - no suitable route found`)

    const {state, next} = await readState(pgClient, stream, applyEvent, createInitialState())
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

    return appendEvents(pgClient, `aggregate.${name}`, stream, next, recordedEvents)
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

function createTypeMapping (aggregates) {
  const index = {}

  for (const name in aggregates) {
    const aggregate = aggregates[name]
    const {commandTypes} = aggregate

    commandTypes.forEach(type => { index[type] = {aggregate, name} })
  }

  return index
}

async function readState (pgClient, stream, applyEvent, state) {
  const events = readEventsByStream(pgClient, stream)
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
