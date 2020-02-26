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

async function createContext (logger, options = {}) {
  const {context, timeout} = options
  const doneHandlers = []
  let disconnectContext, doneError, donePromise, doneReject, timeoutId

  if (typeof timeout === 'number') {
    timeoutId = setTimeout(async () => {
      try {
        await markDone(new TimedOut())
      } catch (error) {}
    }, timeout)
  }

  if (context) disconnectContext = await context.onceDone(markDone)

  return {
    async cancel () {
      await markDone(new Canceled())
    },

    async check () {
      if (doneError) throw doneError
    },

    do: _do,

    async doPromise (fn) {
      return _do(() => new Promise(fn))
    },

    async onceDone (doneHandler) {
      if (doneError) {
        await doneHandler(doneError)

        throw doneError
      }

      const wrapper = [doneHandler]
      doneHandlers.unshift(wrapper)

      return () => {
        const index = doneHandlers.indexOf(wrapper)
        if (index >= 0) doneHandlers.splice(index, 1)
      }
    },
  }

  async function _do (fn) {
    if (doneError) throw doneError
    if (!donePromise) donePromise = new Promise((resolve, reject) => { doneReject = reject })

    return Promise.race([donePromise, fn()])
  }

  async function markDone (error) {
    if (doneError) return

    doneError = error

    if (timeoutId) clearTimeout(timeoutId)
    if (disconnectContext) disconnectContext()
    if (doneReject) doneReject(doneError)

    for (const [doneHandler] of doneHandlers) await doneHandler(doneError)
  }
}
