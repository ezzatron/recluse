const {createStateController} = require('./state.js')
const {executeCommands: executeCommandsRaw} = require('./command.js')
const {maintainProjection} = require('./projection.js')
const {PROCESS} = require('./handler.js')

module.exports = {
  maintainProcess,
}

function maintainProcess (serialization, pgPool, name, process, options = {}) {
  const {copy} = serialization
  const processType = `process.${name}`
  const {commandTypes, createInitialState, eventTypes, handleEvent, routeEvent} = process
  const {timeout, clock} = options

  async function applyEvent (pgClient, event) {
    if (!eventTypes.includes(event.type)) return false

    const instance = routeEvent(event)

    if (!instance) return false

    const executedCommands = []

    function executeCommands (...commands) {
      commands.forEach(command => {
        const {type: commandType} = command

        if (!commandTypes.includes(commandType)) {
          throw new Error(`Process ${name} cannot execute ${commandType} commands`)
        }

        executedCommands.push(command)
      })
    }

    const {getState, isUpdated, readState, updateState} = createStateController(
      copy,
      async () => readProcessState(serialization, pgClient, name, processType, instance, createInitialState),
    )

    await handleEvent({event, executeCommands, readState, updateState})
    await executeCommandsRaw(serialization, pgClient, processType, executedCommands)

    if (isUpdated()) {
      const state = await getState()
      await writeState(serialization, pgClient, name, processType, instance, state)
    }

    return true
  }

  return maintainProjection(serialization, pgPool, processType, {applyEvent}, {clock, timeout, type: processType})
}

async function readProcessState (serialization, pgClient, name, processType, instance, createInitialState) {
  const result = await pgClient.query(
    'SELECT state FROM recluse.process WHERE type = $1 AND instance = $2',
    [processType, instance],
  )

  if (result.rowCount > 0) {
    const {unserialize} = serialization

    return unserialize(result.rows[0].state, PROCESS, name)
  }

  return createInitialState()
}

async function writeState (serialization, pgClient, name, processType, instance, state) {
  const {serialize} = serialization

  await pgClient.query(
    `
    INSERT INTO recluse.process (type, instance, state) VALUES ($1, $2, $3)
    ON CONFLICT (type, instance) DO UPDATE SET state = $3
    `,
    [processType, instance, serialize(state, PROCESS, name)],
  )
}
