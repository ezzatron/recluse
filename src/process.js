const {executeCommands: executeCommandsRaw} = require('./command.js')
const {maintainProjection} = require('./projection.js')
const {PROCESS} = require('./handler.js')

module.exports = {
  maintainProcess,
}

function maintainProcess (serialization, pgPool, name, process, options = {}) {
  const processType = `process.${name}`
  const {commandTypes, createInitialState, eventTypes, handleEvent, routeEvent} = process
  const {timeout, clock} = options

  async function apply (pgClient, event) {
    if (!eventTypes.includes(event.type)) return false

    const instance = routeEvent(event)

    if (!instance) return false

    const state = await readState(serialization, pgClient, name, processType, instance, createInitialState)

    let shouldReplaceState = false
    let nextState
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

    function replaceState (state) {
      shouldReplaceState = true
      nextState = state
    }

    await handleEvent({event, executeCommands, replaceState, state})
    await executeCommandsRaw(serialization, pgClient, processType, executedCommands)
    if (shouldReplaceState) await writeState(serialization, pgClient, name, processType, instance, nextState)

    return true
  }

  return maintainProjection(serialization, pgPool, processType, apply, {timeout, clock})
}

async function readState (serialization, pgClient, name, processType, instance, createInitialState) {
  const result = await pgClient.query(
    'SELECT state FROM recluse.process WHERE type = $1 AND instance = $2',
    [processType, instance]
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
    [processType, instance, serialize(state, PROCESS, name)]
  )
}
