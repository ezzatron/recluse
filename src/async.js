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

const CONTEXT = Symbol('CONTEXT')

/**
 * Throws an exception if the supplied context is "done".
 */
function assertRunning (context) {
  if (!(context && context[CONTEXT])) throw new Error('Invalid context supplied')

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
      [CONTEXT]: true,

      /**
       * A promise that rejects when the context becomes "done".
       */
      get done () {
        if (!done) done = new Promise((resolve, reject) => { doneReject = reject })

        return done
      },

      /**
       * The "done" error, or undefined if the context is still running.
       */
      get doneError () {
        return doneError
      },

      /**
       * Register a function to be executed when this context transitions to
       * "done".
       */
      onceDone (doneHandler) {
        const wrapper = [doneHandler]
        doneHandlers.push(wrapper)

        return function removeDoneHandler () {
          const index = doneHandlers.indexOf(wrapper)
          if (index >= 0) doneHandlers.splice(index, 1)
        }
      },
    },

    /**
     * Cancels this context and any child contexts.
     */
    function cancel () {
      markDone(new Canceled())
    },
  ]

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

  let promise, resolvePromiseAvailable
  const promiseAvailable = new Promise(resolve => { resolvePromiseAvailable = resolve })

  const removeCleanup = cleanup && context.onceDone(doneError => {
    promiseAvailable.then(() => cleanup(promise, doneError))
  })

  async function executeAndRemoveCleanup () {
    promise = (async () => fn())()
    resolvePromiseAvailable()

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

/**
 * Returns a boolean indicating whether the supplied error is a "canceled"
 * error.
 */
function isCanceled (error) {
  return error instanceof Canceled
}

/**
 * Returns a boolean indicating whether the supplied error is a "canceled" or
 * "timed out" error.
 */
function isDone (error) {
  return error instanceof Done
}

/**
 * Returns a boolean indicating whether the supplied error is a "timed out"
 * error.
 */
function isTimedOut (error) {
  return error instanceof TimedOut
}

/**
 * Runs an async function and manages deferred logic.
 *
 * The supplied function will be called with a "defer" callback. This callback
 * can be used to register async "cleanup" callbacks to run once the main
 * function has completed.
 *
 * The cleanup callbacks are executed in reverse order of registration. Each
 * cleanup callback will be called with a sync "recover" callback. Calling this
 * callback will return any error thrown by either the main function, or one of
 * the previously executed cleanup callbacks.
 *
 * Calling the recover callback also causes the error to be considered
 * "handled", and hence should be re-thrown by the caller of the recover
 * callback if the error should continue to propagate.
 *
 * Cleanup callbacks will continue to be executed even if a previous error was
 * not handled. If an error is never handled by calling the recover callback, it
 * will be thrown at the end of execution.
 *
 * Errors thrown from cleanup callbacks will take precedence over any previous
 * unhandled errors. That is, only the "last" error will be thrown.
 */
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
