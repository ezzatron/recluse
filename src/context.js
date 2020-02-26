const {systemClock} = require('./clock.js')

class Canceled extends Error {
  constructor () {
    super('Canceled')
  }
}

class TimedOut extends Error {
  constructor () {
    super('Timed out')
  }
}

module.exports = {
  Canceled,
  createContext,
  TimedOut,
}

async function createContext (options = {}) {
  const {clock = systemClock, context, timeout} = options
  const doneHandlers = []
  let doneError, donePromise, doneReject, timeoutId

  async function markDone (error) {
    if (doneError) return

    doneError = error

    if (timeoutId) clock.clearTimeout(timeoutId)
    if (doneReject) doneReject(doneError)

    for (const doneHandler of doneHandlers) await doneHandler(doneError)
  }

  if (typeof timeout === 'number') {
    timeoutId = clock.setTimeout(async () => {
      try {
        await markDone(new TimedOut())
      } catch (error) {}
    }, timeout)
  }

  if (context) await context.onceDone(markDone)

  async function doAsync (fn) {
    if (doneError) throw doneError
    if (!donePromise) donePromise = new Promise((resolve, reject) => { doneReject = reject })

    return Promise.race([donePromise, fn()])
  }

  return {
    async cancel () {
      await markDone(new Canceled())
    },

    check () {
      if (doneError) throw doneError
    },

    do: doAsync,

    async promise (fn) {
      return doAsync(() => new Promise(fn))
    },

    async onceDone (doneHandler) {
      if (doneError) {
        await doneHandler(doneError)

        throw doneError
      }

      doneHandlers.unshift(doneHandler)
    },
  }
}
