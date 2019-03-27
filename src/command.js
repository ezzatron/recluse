const {appendEvents, readEventsByStream} = require('./event.js')

module.exports = {
  createCommandHandler,
}

function createCommandHandler (aggregates) {
  const mapping = createTypeMapping(aggregates)

  return async function handleCommand (pgClient, command) {
    const {type} = command
    const mapped = mapping[type]

    if (!mapped) throw new Error(`Unable to handle ${type} command - no suitable aggregates found`)

    const {aggregate, name} = mapped
    const {applyEvent, createInitialState, eventTypes, handleCommand, routeCommand, type: aggregateType} = aggregate
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

    return appendEvents(pgClient, aggregateType, stream, next, recordedEvents)
  }
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
