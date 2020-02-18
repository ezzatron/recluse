const {readEventsContinuously} = require('./event.js')
const {acquireAsyncIterator} = require('./iterator.js')
const {PROJECTION: LOCK_NAMESPACE} = require('./lock.js')
const {acquireSessionLock, inTransaction, releaseSessionLock} = require('./pg.js')

module.exports = {
  maintainProjection,
}

function maintainProjection (logger, serialization, pgPool, name, projection, options = {}) {
  const {clock, timeout, type = `projection.${name}`} = options
  const iterator = createProjectionIterator(logger, serialization, pgPool, type, projection, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

function createProjectionIterator (logger, serialization, pgPool, type, projection, timeout, clock) {
  const {applyEvent} = projection
  let id, start, iterator, pgClient
  let isLocked = false

  return {
    async next () {
      if (!iterator) {
        logger.debug('Acquiring Postgres client for event iteration')
        pgClient = await pgPool.connect()

        logger.debug('Acquiring session lock for projection maintenance')
        id = await readProjectionId(pgClient, type)
        await acquireSessionLock(pgClient, LOCK_NAMESPACE, id)
        isLocked = true
        logger.debug('Acquired session lock for projection maintenance')

        logger.debug('Creating event iterator')
        start = await readProjectionNext(pgClient, id)
        iterator = acquireAsyncIterator(
          readEventsContinuously(logger, serialization, pgClient, {start, timeout, clock}),
        )
      }

      logger.debug('Awaiting event')
      const {done, value: wrapper} = await iterator.next()

      if (done) {
        logger.debug('Event iterator ended, stopping projection maintenance')

        return {done: true}
      }

      const {event} = wrapper
      const value = await apply(logger, pgPool, type, applyEvent, start++, event)

      return {done: false, value}
    },

    async cancel () {
      const errors = []

      logger.debug('Cancelling projection maintainer')

      if (iterator) {
        logger.debug('Cancelling event iterator')

        try {
          await iterator.cancel()
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to cancel event iterator: ${error.stack}`)
        }
      } else {
        logger.debug('No event iterator to cancel')
      }

      if (isLocked) {
        logger.debug('Releasing session lock for projection maintenance')

        try {
          await releaseSessionLock(pgClient, LOCK_NAMESPACE)
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to release session lock for projection maintenance: ${error.stack}`)
        }
      } else {
        logger.debug('A session lock for projection maintenance was never acquired')
      }

      if (pgClient) {
        logger.debug('Releasing Postgres client for projection maintenance')

        try {
          pgClient.release()
        } catch (error) {
          errors.push(error)
          logger.warn(`Failed to release Postgres client for projection maintenance: ${error.stack}`)
        }
      } else {
        logger.debug('A Postgres client for projection maintenance was never acquired')
      }

      if (errors.length > 0) throw errors[0]

      logger.debug('Cancelled projection maintainer')
    },
  }
}

async function apply (logger, pgPool, type, applyEvent, offset, event) {
  const {type: eventType} = event

  logger.debug(`Consuming ${eventType} event`)

  logger.debug(`Acquiring Postgres client to handle ${eventType} event`)
  const pgClient = await pgPool.connect()

  try {
    return await inTransaction(pgClient, async () => {
      await incrementProjection(pgClient, type, offset)

      const result = await applyEvent(pgClient, event)

      if (result) logger.info(`Applied ${eventType} event with ${type}`)

      return result
    })
  } finally {
    logger.debug(`Releasing Postgres client used to handle ${eventType} event`)
    pgClient.release()
  }
}

async function readProjectionId (pgClient, type) {
  const result = await pgClient.query(
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

async function readProjectionNext (pgClient, id) {
  const result = await pgClient.query(
    'SELECT next FROM recluse.projection WHERE id = $1',
    [id],
  )

  if (result.rowCount < 1) throw new Error('Unable to read next projection offset')

  return parseInt(result.rows[0].next)
}

async function incrementProjection (pgClient, type, offset) {
  const result = await pgClient.query(
    'UPDATE recluse.projection SET next = $2 + 1 WHERE type = $1 AND next = $2',
    [type, offset],
  )

  if (result.rowCount !== 1) throw new Error('Unable to lock projection for updating')
}
