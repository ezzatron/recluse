const {executeCommands: executeCommandsRaw} = require('./command.js')
const {query} = require('./pg.js')
const {maintainProjection} = require('./projection.js')
const {createStateController} = require('./state.js')

module.exports = {
  maintainProcess,
}

async function maintainProcess (context, logger, pool, serialization, name, process, options = {}) {
  const processType = `process.${name}`
  const {commandTypes, createInitialState, eventTypes, handleEvent, routeEvent} = process
  const {shouldContinue, timeout} = options

  async function applyEvent ({client, context, event, logger}) {
    const {type: eventType} = event

    if (!eventTypes.includes(eventType)) return

    const instance = routeEvent(event)

    if (!instance) return

    const executedCommands = []

    function executeCommands (...commands) {
      for (const command of commands) {
        const {type: commandType} = command

        if (!commandTypes.includes(commandType)) {
          throw new Error(`Process ${name} cannot execute ${commandType} commands`)
        }

        logger.debug(`Recording ${commandType} command from ${processType}`)
        executedCommands.push(command)
      }
    }

    const {getState, isUpdated, readState, updateState} = createStateController(
      serialization,
      async () => readProcessState(context, logger, client, serialization, processType, instance, createInitialState),
    )

    logger.debug(`Handling ${eventType} event with ${processType}`)
    await handleEvent({event, executeCommands, readState, updateState})

    logger.debug(`Executing ${executedCommands.length} commands for ${processType}`)
    await executeCommandsRaw(context, logger, client, serialization, processType, executedCommands)

    if (isUpdated()) {
      logger.debug(`Writing state changes caused by ${eventType} in ${processType}`)
      const state = await getState()
      await writeState(context, logger, client, serialization, processType, instance, state)
    } else {
      logger.debug(`No state changes caused by ${eventType} in ${processType}`)
    }
  }

  await maintainProjection(
    context,
    logger,
    pool,
    serialization,
    processType,
    {applyEvent},
    {shouldContinue, timeout, type: processType},
  )
}

async function readProcessState (context, logger, client, serialization, processType, instance, createInitialState) {
  const {unserialize} = serialization

  const result = await query(
    context,
    logger,
    client,
    'SELECT state FROM recluse.process WHERE type = $1 AND instance = $2',
    [processType, instance],
  )

  if (result.rowCount > 0) return unserialize(result.rows[0].state)

  return createInitialState()
}

async function writeState (context, logger, client, serialization, processType, instance, state) {
  const {serialize} = serialization

  await query(
    context,
    logger,
    client,
    `
    INSERT INTO recluse.process (type, instance, state) VALUES ($1, $2, $3)
    ON CONFLICT (type, instance) DO UPDATE SET state = $3
    `,
    [processType, instance, serialize(state)],
  )
}
