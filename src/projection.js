const {acquireAsyncIterator} = require('./iterator.js')
const {acquireSessionLock, inTransaction, releaseSessionLock} = require('./pg.js')
const {allSerial} = require('./async.js')
const {PROJECTION: LOCK_NAMESPACE} = require('./lock.js')
const {readEventsContinuously} = require('./event.js')

module.exports = {
  maintainProjection,
}

function maintainProjection (serialization, pgPool, name, projection, options = {}) {
  const {clock, timeout, type = `projection.${name}`} = options
  const iterator = createProjectionIterator(serialization, pgPool, type, projection, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

function createProjectionIterator (serialization, pgPool, type, projection, timeout, clock) {
  const {applyEvent} = projection
  let id, start, iterator, pgClient
  let isLocked = false

  return {
    async next () {
      if (!iterator) {
        pgClient = await pgPool.connect()

        id = await readProjectionId(pgClient, type)
        await acquireSessionLock(pgClient, LOCK_NAMESPACE, id)
        isLocked = true

        start = await readProjectionNext(pgClient, id)
        iterator = acquireAsyncIterator(readEventsContinuously(serialization, pgClient, {start, timeout, clock}))
      }

      const {value: {event}} = await iterator.next()
      const value = await apply(pgPool, type, applyEvent, start++, event)

      return {done: false, value}
    },

    async cancel () {
      await allSerial(
        async () => { if (iterator) await iterator.cancel() },
        async () => { if (isLocked) await releaseSessionLock(pgClient, LOCK_NAMESPACE, id) },
        () => { if (pgClient) pgClient.release() },
      )
    },
  }
}

async function apply (pgPool, type, applyEvent, offset, event) {
  const pgClient = await pgPool.connect()

  try {
    return inTransaction(pgClient, async () => {
      await incrementProjection(pgClient, type, offset)

      return applyEvent(pgClient, event)
    })
  } finally {
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
    [type]
  )

  if (result.rowCount < 1) throw new Error('Unable to read projection ID')

  return parseInt(result.rows[0].id)
}

async function readProjectionNext (pgClient, id) {
  const result = await pgClient.query(
    'SELECT next FROM recluse.projection WHERE id = $1',
    [id]
  )

  if (result.rowCount < 1) throw new Error('Unable to read next projection offset')

  return parseInt(result.rows[0].next)
}

async function incrementProjection (pgClient, type, offset) {
  const result = await pgClient.query(
    'UPDATE recluse.projection SET next = $2 + 1 WHERE type = $1 AND next = $2',
    [type, offset]
  )

  if (result.rowCount !== 1) throw new Error('Unable to lock projection for updating')
}
