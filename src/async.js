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
  withDefer,
}

/**
 * Creates a context for performing asynchronous work.
 *
 * Supports nesting of contexts, cancellation, and timeouts. Inspired by
 * Golang's contexts.
 */
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
    /**
     * Cancels this context and any child contexts.
     */
    async cancel () {
      await markDone(new Canceled())
    },

    /**
     * If this context is "done", throws an error representing the cause.
     */
    async check () {
      if (doneError) throw doneError
    },

    /**
     * Executes the supplied async function, but if this context becomes "done"
     * before completion, throws an error representing the cause.
     */
    do: _do,

    /**
     * Same as context.do(), but accepts a Promise "executor" instead of an
     * async function.
     *
     * The promise "executor" is the function typically passed to the Promise
     * constructor that accepts the resolve and reject functions as arguments.
     */
    async doPromise (executor) {
      return _do(() => new Promise(executor))
    },

    /**
     * Register an async function to be executed when this context transitions
     * to "done".
     *
     * When canceling a context, all done handlers must complete before the
     * call to context.cancel() will return.
     */
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

async function withDefer (fn) {
  const deferreds = []
  let isDone = false
  let result, lastError

  const defer = deferred => {
    if (isDone) throw new Error('Defer called after main function completed')

    deferreds.unshift(deferred)
  }

  try {
    result = await fn(defer)
  } catch (error) {
    lastError = error
  }

  isDone = true

  for (const deferred of deferreds) {
    try {
      await deferred(() => {
        const error = lastError
        lastError = undefined

        return error
      })
    } catch (error) {
      lastError = error
    }
  }

  if (lastError) throw lastError

  return result
}
