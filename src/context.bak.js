const {systemClock} = require('./clock.js')

class Aborted extends Error {
  constructor (reason) {
    super('Aborted')

    Object.defineProperty(this, 'reason', {value: reason, writable: false})
  }
}

const CANCELED = Symbol('CANCELED')
const TIMED_OUT = Symbol('TIMED_OUT')

module.exports = {
  Aborted,
  CANCELED,
  createContext,
  TIMED_OUT,
}

function createContext (options = {}) {
  const {clock = systemClock, parent, timeout} = options
  const routines = []
  let cleanup, error, timeoutId
  let isDone = false

  const done = new Promise((resolve, reject) => {
    cleanup = async reason => {
      if (isDone) return error

      isDone = true
      if (timeoutId) clock.clearTimeout(timeoutId)

      try {
        await Promise.all(routines.map(({cleanup, value}) => cleanup(value)))
        error = new Aborted(reason)
      } catch (cleanupError) {
        error = cleanupError
      }

      reject(error)

      return error
    }

    if (timeout) timeoutId = clock.setTimeout(() => cleanup(TIMED_OUT), timeout)
  })
  done.catch(() => {})

  return {
    async cancel () {
      const cleanupError = await cleanup(CANCELED)

      if (cleanupError) throw cleanupError
    },

    async do (promise, cleanup) {
      const routine = {cleanup}
      routines.push(routine)
      promise.then(value => { routine.value = value })

      return Promise.race([done, promise])
    },

    get isDone () {
      return isDone
    },
  }
}

async function doThing (context) {
  const operation = // ...

  const lock = await context.do(aquireLock(), async promise => {
    operation.cancel()

    let lock

    try {
      lock = await promise
    } catch (error) {
      return
    }

    await lock.release()
  })
}
