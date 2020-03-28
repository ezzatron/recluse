const {
  assertRunning,
  createContext,
  doInterminable,
  doTerminable,
  isCanceled,
  isDone,
  isTimedOut,
} = require('../../../src/async.js')

const {createLogger} = require('../../helper/logging.js')

describe('Async context', () => {
  let cancel
  let cancelAsync
  let context
  let createCleanup
  let log
  let logger
  let rejectWithError
  let resolveToValue
  let returnValue
  let runAllJestTimers
  let runPromises
  let sleep
  let throwError

  beforeEach(() => {
    jest.useFakeTimers()

    sleep = delay => new Promise(resolve => { setTimeout(resolve, delay) })
    runPromises = () => new Promise(resolve => { setImmediate(resolve) })
    runAllJestTimers = async () => { jest.runAllTimers() }

    log = []
    logger = createLogger()

    const created = createContext(logger)
    context = created[0]
    cancel = created[1]

    cancelAsync = async () => { cancel() }

    rejectWithError = async () => {
      log.push('rejectWithError started')
      await sleep(111)
      log.push('rejectWithError completed')

      throw new Error('error-a')
    }

    resolveToValue = async () => {
      log.push('resolveToValue started')
      await sleep(111)
      log.push('resolveToValue completed')

      return 'value-a'
    }

    returnValue = () => {
      log.push('returnValue completed')

      return 'value-a'
    }

    throwError = () => {
      log.push('throwError completed')

      throw new Error('error-a')
    }

    createCleanup = label => {
      return async (promise, doneError) => {
        log.push(`cleanup ${label} started because: ${doneError.message}`)

        await sleep(111)

        try {
          const result = await promise
          log.push(`cleanup ${label} completed with value ${result}`)
        } catch (error) {
          log.push(`cleanup ${label} completed with error ${error.message}`)
        }
      }
    }
  })

  afterEach(() => {
    cancel()
  })

  describe('assertRunning()', () => {
    it('should not throw an error if the context is running', () => {
      expect(() => assertRunning(context)).not.toThrow()
    })

    it('should throw an error if the context is not running', () => {
      cancel()
      expect(() => assertRunning(context)).toThrow('Canceled')
    })

    it('should throw an error if a non-context is supplied', () => {
      expect(() => assertRunning()).toThrow('Invalid context supplied')
      expect(() => assertRunning({})).toThrow('Invalid context supplied')
    })
  })

  describe('doInterminable()', () => {
    it('should support running interminable functions with cleanup', async () => {
      const cleanup = jest.fn()

      await Promise.all([
        expect(doInterminable(context, resolveToValue, cleanup)).resolves.toBe('value-a'),
        expect(doInterminable(context, returnValue, cleanup)).resolves.toBe('value-a'),
        expect(doInterminable(context, rejectWithError, cleanup)).rejects.toThrow('error-a'),
        expect(doInterminable(context, throwError, cleanup)).rejects.toThrow('error-a'),
        runAllJestTimers(),
      ])

      expect(cleanup).not.toHaveBeenCalled()
    })

    it('should support running interminable functions without cleanup', async () => {
      await Promise.all([
        expect(doInterminable(context, resolveToValue)).resolves.toBe('value-a'),
        expect(doInterminable(context, returnValue)).resolves.toBe('value-a'),
        expect(doInterminable(context, rejectWithError)).rejects.toThrow('error-a'),
        expect(doInterminable(context, throwError)).rejects.toThrow('error-a'),
        runAllJestTimers(),
      ])
    })

    it('should support cleaning up after exiting early from interminable functions that resolve', async () => {
      const cleanupResolveToValue = createCleanup('resolveToValue')
      const task = doInterminable(context, resolveToValue, cleanupResolveToValue)
        .finally(() => { log.push('resolveToValue exited') })

      await Promise.all([
        expect(task).rejects.toThrow('Canceled'),
        cancelAsync(),
      ])

      jest.runAllTimers()
      await runPromises()
      jest.runAllTimers()

      expect(log).toEqual([
        'resolveToValue started',
        'cleanup resolveToValue started because: Canceled',
        'resolveToValue exited',
        'resolveToValue completed',
        'cleanup resolveToValue completed with value value-a',
      ])
    })

    it('should support cleaning up after exiting early from interminable functions that reject', async () => {
      const cleanupRejectWithError = createCleanup('rejectWithError')
      const task = doInterminable(context, rejectWithError, cleanupRejectWithError)
        .finally(() => { log.push('rejectWithError exited') })

      await Promise.all([
        expect(task).rejects.toThrow('Canceled'),
        cancelAsync(),
      ])

      jest.runAllTimers()
      await runPromises()
      jest.runAllTimers()

      expect(log).toEqual([
        'rejectWithError started',
        'cleanup rejectWithError started because: Canceled',
        'rejectWithError exited',
        'rejectWithError completed',
        'cleanup rejectWithError completed with error error-a',
      ])
    })

    it('should support exiting early from interminable functions that resolve, but have no cleanup', async () => {
      const task = doInterminable(context, resolveToValue)
        .finally(() => { log.push('resolveToValue exited') })

      await Promise.all([
        expect(task).rejects.toThrow('Canceled'),
        cancelAsync(),
      ])

      jest.runAllTimers()
      await runPromises()
      jest.runAllTimers()

      expect(log).toEqual([
        'resolveToValue started',
        'resolveToValue exited',
        'resolveToValue completed',
      ])
    })

    it('should support exiting early from interminable functions that reject, but have no cleanup', async () => {
      const task = doInterminable(context, rejectWithError)
        .finally(() => { log.push('rejectWithError exited') })

      await Promise.all([
        expect(task).rejects.toThrow('Canceled'),
        cancelAsync(),
      ])

      jest.runAllTimers()
      await runPromises()
      jest.runAllTimers()

      expect(log).toEqual([
        'rejectWithError started',
        'rejectWithError exited',
        'rejectWithError completed',
      ])
    })

    it('should not run functions on a context that is already done', async () => {
      const fn = jest.fn()
      const cleanup = jest.fn()
      cancel()

      await expect(doInterminable(context, fn, cleanup)).rejects.toThrow('Canceled')
      expect(fn).not.toHaveBeenCalled()
      expect(cleanup).not.toHaveBeenCalled()
    })
  })

  describe('doTerminable()', () => {
    it('should support running functions', async () => {
      const abort = () => {}

      await Promise.all([
        expect(doTerminable(context, resolveToValue, abort)).resolves.toBe('value-a'),
        expect(doTerminable(context, returnValue, abort)).resolves.toBe('value-a'),
        expect(doTerminable(context, rejectWithError, abort)).rejects.toThrow('error-a'),
        expect(doTerminable(context, throwError, abort)).rejects.toThrow('error-a'),
        runAllJestTimers(),
      ])
    })

    it('should support aborting functions', async () => {
      function defer () {
        let res, rej
        const promise = new Promise((resolve, reject) => {
          res = resolve
          rej = reject
        })

        return [() => promise, res, rej]
      }

      const [fnA, resolveA] = defer()
      const [fnB, , rejectB] = defer()
      const abortA = () => { resolveA('value-a') }
      const abortB = () => { rejectB(new Error('error-a')) }

      await Promise.all([
        expect(doTerminable(context, fnA, abortA)).resolves.toBe('value-a'),
        expect(doTerminable(context, fnB, abortB)).rejects.toThrow('error-a'),
        cancelAsync(),
      ])
    })

    it('should not run functions on a context that is already done', async () => {
      const fn = jest.fn()
      const abort = jest.fn()
      cancel()

      await expect(doTerminable(context, fn, abort)).rejects.toThrow('Canceled')
      expect(fn).not.toHaveBeenCalled()
      expect(abort).not.toHaveBeenCalled()
    })
  })

  describe('isCanceled', () => {
    it('should correctly identify Canceled errors', () => {
      const [timeoutContext] = createContext(logger, {context, timeout: 111})
      jest.runAllTimers()
      cancel()

      expect(isCanceled(context.doneError)).toBe(true)
      expect(isCanceled(timeoutContext.doneError)).toBe(false)
      expect(isCanceled(new Error())).toBe(false)
    })
  })

  describe('isDone', () => {
    it('should correctly identify Done errors', () => {
      const [timeoutContext] = createContext(logger, {context, timeout: 111})
      jest.runAllTimers()
      cancel()

      expect(isDone(timeoutContext.doneError)).toBe(true)
      expect(isDone(context.doneError)).toBe(true)
      expect(isDone(new Error())).toBe(false)
    })
  })

  describe('isTimedOut', () => {
    it('should correctly identify TimedOut errors', () => {
      const [timeoutContext] = createContext(logger, {context, timeout: 111})
      jest.runAllTimers()
      cancel()

      expect(isTimedOut(timeoutContext.doneError)).toBe(true)
      expect(isTimedOut(context.doneError)).toBe(false)
      expect(isTimedOut(new Error())).toBe(false)
    })
  })
})
