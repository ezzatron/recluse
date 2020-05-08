const {readEventsContinuously} = require('./event.js')
const {PROJECTION: LOCK_NAMESPACE} = require('./lock.js')
const {inPoolTransaction, query, withAdvisoryLock, withClient} = require('./pg.js')

module.exports = {
  maintainProjection,
}

async function maintainProjection (context, logger, pool, serialization, name, projection, options = {}) {
  const {applyEvent} = projection
  const {shouldContinue, timeout, type = `projection.${name}`} = options

  logger.debug(`Acquiring client for ${type} maintenance`)

  await withClient(context, logger, pool, async client => {
    logger.debug(`Acquired client for ${type} maintenance`)
    logger.debug(`Acquiring session lock for ${type} maintenance`)

    const id = await readProjectionId(context, logger, client, type)

    await withAdvisoryLock(context, logger, pool, LOCK_NAMESPACE, id, async () => {
      logger.debug(`Acquired session lock for ${type} maintenance`)

      let start = await readProjectionNext(context, logger, client, id)

      await readEventsContinuously(context, logger, client, serialization, {start, timeout}, async ({event}) => {
        await consumeEvent(context, logger, pool, type, applyEvent, start++, event)

        if (shouldContinue && !shouldContinue()) return false

        logger.debug(`Awaiting event for ${type}`)

        return true
      })
    })
  })
}

async function consumeEvent (context, logger, pool, type, applyEvent, offset, event) {
  const {type: eventType} = event
  logger.debug(`Consuming ${eventType} event with ${type}`)

  await inPoolTransaction(context, logger, pool, async client => {
    logger.debug(`Incrementing ${type} for ${eventType} event`)
    await incrementProjection(context, logger, client, type, offset)

    logger.debug(`Applying ${eventType} event to ${type}`)
    await applyEvent({client, context, event, logger})

    logger.info(`Consumed ${eventType} event with ${type}`)
  })
}

async function readProjectionId (context, logger, client, type) {
  const result = await query(
    context,
    logger,
    client,
    `
    INSERT INTO recluse.projection (type, next) VALUES ($1, 0)
    ON CONFLICT (type) DO UPDATE SET type = $1
    RETURNING id
    `,
    [type],
  )

  if (result.rowCount < 1) throw new Error('Unable to read projection ID')

  return parseInt(result.rows[0].id)
}

async function readProjectionNext (context, logger, client, id) {
  const result = await query(
    context,
    logger,
    client,
    'SELECT next FROM recluse.projection WHERE id = $1',
    [id],
  )

  if (result.rowCount < 1) throw new Error('Unable to read next projection offset')

  return parseInt(result.rows[0].next)
}

async function incrementProjection (context, logger, client, type, offset) {
  const result = await query(
    context,
    logger,
    client,
    'UPDATE recluse.projection SET next = $2 + 1 WHERE type = $1 AND next = $2',
    [type, offset],
  )

  if (result.rowCount !== 1) throw new Error('Unable to lock projection for updating')
}
