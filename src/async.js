class Done extends Error {}

class Canceled extends Done {
  constructor () {
    super('Canceled')
  }
}

class TimedOut extends Done {
  constructor () {
    super('Timed out')
  }
}

module.exports = {
  assertRunning,
  createContext,
  doTerminable,
  doInterminable,
  isCanceled,
  isDone,
  isTimedOut,
  withDefer,
}

function assertRunning (context) {
  const {doneError} = context

  if (doneError) throw doneError
}

/**
 * Creates a context for performing asynchronous work.
 *
 * Supports nesting of contexts, cancellation, and timeouts. Inspired by
 * Golang's contexts.
 */
function createContext (logger, options = {}) {
  const {context, timeout} = options
  let doneHandlers = []
  let disconnectContext, done, doneError, doneReject, timeoutId

  if (typeof timeout === 'number') {
    timeoutId = setTimeout(() => { markDone(new TimedOut()) }, timeout)
  }

  if (context) disconnectContext = context.onceDone(markDone)

  return [
    {
      get done () {
        if (!done) done = new Promise((resolve, reject) => { doneReject = reject })

        return done
      },

      get doneError () {
        return doneError
      },

      onceDone,
    },
    cancel,
  ]

  /**
   * Cancels this context and any child contexts.
   */
  function cancel () {
    markDone(new Canceled())
  }

  /**
   * Register a function to be executed when this context transitions to "done".
   */
  function onceDone (doneHandler) {
    const wrapper = [doneHandler]
    doneHandlers.push(wrapper)

    return function removeDoneHandler () {
      const index = doneHandlers.indexOf(wrapper)
      if (index >= 0) doneHandlers.splice(index, 1)
    }
  }

  /**
   * Used internally by both cancellation and timeouts to transition this
   * context to "done".
   */
  function markDone (error) {
    if (doneError) return

    doneError = error

    if (timeoutId) clearTimeout(timeoutId)
    if (disconnectContext) disconnectContext()

    for (const [doneHandler] of doneHandlers) doneHandler(doneError)
    doneHandlers = []

    if (doneReject) doneReject(doneError)
  }
}

/**
 * Runs an async function with no termination mechanism under a context.
 *
 * If the function resolves before the context becomes done, the resolved value
 * is returned, and the cleanup handler is never executed.
 *
 * If the function rejects before the context becomes done, the rejection is
 * re-thrown, and the cleanup handler is never executed.
 *
 * If the context becomes done before the function resolves or rejects, the done
 * error is immediately thrown. The cleanup handler is then called, but is not
 * awaited. For this reason, cleanup handlers should never throw, and doing so
 * will result in undefined behavior.
 *
 * If the cleanup handler is called, it will be called with two arguments:
 * - A promise that, when awaited, will resolve or reject in the same way as the
 *   original call to the supplied function. This should typically happen inside
 *   a try/catch statement in order to prevent the cleanup handler from
 *   throwing.
 * - An error representing the reason why the context has become done.
 */
async function doInterminable (context, fn, cleanup) {
  assertRunning(context)
  let promise
  const removeCleanup = cleanup && context.onceDone(doneError => cleanup(promise, doneError))

  async function executeAndRemoveCleanup () {
    promise = (async () => fn())()

    try {
      return await promise
    } finally {
      if (removeCleanup) removeCleanup()
    }
  }

  return Promise.race([context.done, executeAndRemoveCleanup()])
}

/**
 * Runs an async function with termination mechanics under a context.
 *
 * If the function resolves before the context becomes done, the resolved value
 * is returned, and the abort handler is never executed.
 *
 * If the function rejects before the context becomes done, the rejection is
 * re-thrown, and the abort handler is never executed.
 *
 * If the context becomes done before the function resolves or rejects, the
 * abort handler is immediately called. Calling the abort handler should result
 * in the original function call completing in a timely fashion.
 *
 * If the abort handler is called, it will be called with one argument:
 * - An error representing the reason why the context has become done.
 */
async function doTerminable (context, fn, abort) {
  assertRunning(context)
  const removeAbort = context.onceDone(abort)

  try {
    return await fn()
  } finally {
    removeAbort()
  }
}

function isCanceled (error) {
  return error instanceof Canceled
}

function isDone (error) {
  return error instanceof Done
}

function isTimedOut (error) {
  return error instanceof TimedOut
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
