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

    const id = routeEvent(event)

    if (!id) return false

    const state = await readState(pgClient, id, createInitialState)

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
    if (shouldReplaceState) await writeState(pgClient, name, id, nextState)

    return true
  }

  return maintainProjection(pgPool, projectionName, apply, {timeout, clock})
}

async function readState (pgClient, id, createInitialState) {
  const result = await pgClient.query(
    'SELECT data FROM recluse.process WHERE id = $1',
    [id]
  )

  if (result.rowCount > 0) return result.rows[0].data

  return createInitialState()
}

async function writeState (pgClient, name, id, state) {
  await pgClient.query(
    `
    INSERT INTO recluse.process (name, id, data) VALUES ($1, $2, $3)
    ON CONFLICT (name, id) DO UPDATE SET data = $3
    `,
    [name, id, state]
  )
}
