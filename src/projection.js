const {acquireAsyncIterator} = require('./iterator.js')
const {acquireSessionLock, inTransaction, releaseSessionLock} = require('./pg.js')
const {allSerial} = require('./async.js')
const {PROJECTION: LOCK_NAMESPACE} = require('./lock.js')
const {readEventsContinuously} = require('./event.js')

module.exports = {
  maintainProjection,
}

function maintainProjection (pgPool, name, apply, options = {}) {
  const {timeout, clock} = options
  const iterator = createProjectionIterator(pgPool, name, apply, timeout, clock)

  return {
    [Symbol.asyncIterator]: () => iterator,
    cancel: iterator.cancel,
  }
}

function createProjectionIterator (pgPool, name, apply, timeout, clock) {
  let id, offset, iterator, pgClient
  let isLocked = false

  return {
    async next () {
      if (!iterator) {
        pgClient = await pgPool.connect()

        id = await readProjectionId(pgClient, name)
        await acquireSessionLock(pgClient, LOCK_NAMESPACE, id)
        isLocked = true

        offset = await readProjectionNext(pgClient, id)
        iterator = acquireAsyncIterator(readEventsContinuously(pgClient, {offset, timeout, clock}))
      }

      const {value: {event}} = await iterator.next()
      const value = await applyEvent(pgPool, name, apply, offset++, event)

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

async function applyEvent (pgPool, name, apply, offset, event) {
  const pgClient = await pgPool.connect()

  try {
    return inTransaction(pgClient, async () => {
      await incrementProjection(pgClient, name, offset)

      return apply(pgClient, event)
    })
  } finally {
    await pgClient.release()
  }
}

async function readProjectionId (pgClient, name) {
  const result = await inTransaction(pgClient, async () => {
    await pgClient.query(
      `
      INSERT INTO recluse.projection (name, next) VALUES ($1, 0)
      ON CONFLICT (name) DO NOTHING
      `,
      [name]
    )

    return pgClient.query(
      'SELECT id FROM recluse.projection WHERE name = $1',
      [name]
    )
  })

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

async function incrementProjection (pgClient, name, offset) {
  const result = await pgClient.query(
    'UPDATE recluse.projection SET next = $2 + 1 WHERE name = $1 AND next = $2',
    [name, offset]
  )

  if (result.rowCount !== 1) throw new Error('Unable to lock projection for updating')
}
