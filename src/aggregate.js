const {appendEvents, readEventsByStream, readNextStreamOffset} = require('./event.js')
const {createStateController} = require('./state.js')

module.exports = {
  handleCommandWithAggregate,
}

async function handleCommandWithAggregate (context, logger, client, serialization, name, aggregate, command) {
  const {applyEvent, createInitialState, eventTypes, handleCommand, routeCommand} = aggregate
  const {type} = command
  const instance = routeCommand(command)

  if (!instance) throw new Error(`Unable to handle ${type} command - no suitable route found`)

  const streamType = `aggregate.${name}`
  const next = await readNextStreamOffset(context, logger, client, streamType, instance)

  const {readState, updateState} = createStateController(
    serialization,
    async () => {
      return readAggregateState(
        context,
        logger,
        client,
        serialization,
        streamType,
        instance,
        next,
        applyEvent,
        createInitialState,
      )
    },
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

  return appendEvents(context, logger, client, serialization, streamType, instance, next, recordedEvents)
}

async function readAggregateState (
  context,
  logger,
  client,
  serialization,
  streamType,
  instance,
  next,
  applyEvent,
  createInitialState,
) {
  const {getState, readState, updateState} = createStateController(serialization, () => createInitialState())

  await readEventsByStream(
    context,
    logger,
    client,
    serialization,
    streamType,
    instance,
    0,
    Infinity,
    async ({event}) => {
      await applyEvent({event, readState, updateState})

      return true
    },
  )

  return getState()
}
