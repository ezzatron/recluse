const {executeCommands: executeCommandsRaw} = require('./command.js')
const {maintainProjection} = require('./projection.js')

module.exports = {
  maintainProcess,
}

function maintainProcess (pgPool, name, process, options = {}) {
  const processName = `process.${name}`
  const projectionName = `recluse.${processName}`
  const {commandTypes, createInitialState, eventTypes, handleEvent, routeEvent} = process
  const {timeout, clock} = options

  async function apply (pgClient, event) {
    if (!eventTypes.includes(event.type)) return false

    const instance = routeEvent(event)

    if (!instance) return false

    const state = await readState(pgClient, instance, createInitialState)

    let shouldReplaceState = false
    let nextState
    const executedCommands = []

    function executeCommands (...commands) {
      commands.forEach(command => {
        const {type} = command

        if (!commandTypes.includes(type)) throw new Error(`Process ${name} cannot execute ${type} commands`)

        executedCommands.push(command)
      })
    }

    function replaceState (state) {
      shouldReplaceState = true
      nextState = state
    }

    await handleEvent({event, executeCommands, replaceState, state})
    await executeCommandsRaw(pgClient, processName, executedCommands)
    if (shouldReplaceState) await writeState(pgClient, name, instance, nextState)

    return true
  }

  return maintainProjection(pgPool, projectionName, apply, {timeout, clock})
}

async function readState (pgClient, instance, createInitialState) {
  const result = await pgClient.query(
    'SELECT state FROM recluse.process WHERE instance = $1',
    [instance]
  )

  if (result.rowCount > 0) return result.rows[0].state

  return createInitialState()
}

async function writeState (pgClient, name, instance, state) {
  await pgClient.query(
    `
    INSERT INTO recluse.process (name, instance, state) VALUES ($1, $2, $3)
    ON CONFLICT (name, instance) DO UPDATE SET state = $3
    `,
    [name, instance, state]
  )
}
