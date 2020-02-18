const {appendEvents, readEventsByStream, readNextStreamOffset} = require('./event.js')
const {createStateController} = require('./state.js')

module.exports = {
  handleCommandWithAggregate,
}

async function handleCommandWithAggregate (logger, serialization, pgClient, name, aggregate, command) {
  const {copy} = serialization
  const {applyEvent, createInitialState, eventTypes, handleCommand, routeCommand} = aggregate
  const {type} = command
  const instance = routeCommand(command)

  if (!instance) throw new Error(`Unable to handle ${type} command - no suitable route found`)

  const streamType = `aggregate.${name}`
  const next = await readNextStreamOffset(pgClient, streamType, instance)
  const {readState, updateState} = createStateController(
    copy,
    async () => readAggregateState(serialization, pgClient, streamType, instance, next, applyEvent, createInitialState),
  )

  const recordedEvents = []

  async function recordEvents (...events) {
    for (const event of events) {
      const {type} = event

      if (!eventTypes.includes(type)) throw new Error(`Aggregate ${name} cannot record ${type} events`)

      recordedEvents.push(event)
      await applyEvent({event, readState, updateState})
    }
  }

  logger.info(`Handling ${type} command with ${streamType}`)
  await handleCommand({command, readState, recordEvents})

  return appendEvents(serialization, pgClient, streamType, instance, next, recordedEvents)
}

async function readAggregateState (
  serialization,
  pgClient,
  streamType,
  instance,
  next,
  applyEvent,
  createInitialState,
) {
  const {copy} = serialization
  const events = readEventsByStream(serialization, pgClient, streamType, instance, 0, next)
  const {getState, readState, updateState} = createStateController(copy, () => createInitialState())

  for await (const wrapper of events) {
    const {event} = wrapper
    await applyEvent({event, readState, updateState})
  }

  return getState()
}
