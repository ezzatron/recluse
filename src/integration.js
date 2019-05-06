const {appendEventsUnchecked} = require('./event.js')

module.exports = {
  handleCommandWithIntegration,
}

async function handleCommandWithIntegration (serialization, pgClient, name, integration, command) {
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

  return appendEventsUnchecked(serialization, pgClient, streamType, '', recordedEvents)
}
