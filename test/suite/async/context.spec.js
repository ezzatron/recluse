const {Canceled, createContext, TimedOut} = require('../../../src/async.js')
const {createLogger} = require('../../helper/logging.js')

describe('Async context', () => {
  let logger

  beforeEach(() => {
    jest.useFakeTimers()

    logger = createLogger()
  })

  it('should support running async functions', async () => {
    const context = await createContext(logger)

    await expect(context.do(async () => 'value-a')).resolves.toBe('value-a')
    await expect(context.do(async () => { throw new Error('error-a') })).rejects.toThrow('error-a')
  })

  it('should support running callback-based functions', async () => {
    const context = await createContext(logger)

    await expect(context.doPromise(resolve => { resolve('value-a') })).resolves.toBe('value-a')
    await expect(context.doPromise((resolve, reject) => { reject(new Error('error-a')) })).rejects.toThrow('error-a')
  })

  it('should support cancellation before being used', async () => {
    const context = await createContext(logger)

    await expect(context.cancel()).resolves.toBeUndefined()
    await expect(context.check()).rejects.toThrow(Canceled)
  })

  it('should support cancellation whilst running an async function', async () => {
    const context = await createContext(logger)

    await Promise.all([
      expect(context.do(() => new Promise(() => {}))).rejects.toThrow(Canceled),
      context.cancel(),
    ])
  })

  it('should support running async functions when already canceled', async () => {
    const context = await createContext(logger)
    await context.cancel()

    await expect(context.do(() => new Promise(() => {}))).rejects.toThrow(Canceled)
  })

  it('should support cancellation via a parent context', async () => {
    const context = await createContext(logger)
    const childContext = await createContext(logger, {context})

    await Promise.all([
      expect(childContext.do(() => new Promise(() => {}))).rejects.toThrow(Canceled),
      context.cancel(),
    ])
  })

  it('should support cancellation of a child context', async () => {
    const context = await createContext(logger)
    const childContext = await createContext(logger, {context})

    await Promise.all([
      expect(childContext.do(() => new Promise(() => {}))).rejects.toThrow(Canceled),
      childContext.cancel(),
    ])

    await expect(context.check()).resolves.toBeUndefined()
  })

  it('should support timeouts before being used', async () => {
    const context = await createContext(logger, {timeout: 111})

    await expect(context.check()).resolves.toBeUndefined()

    jest.runAllTimers()

    await expect(context.check()).rejects.toThrow(TimedOut)
  })

  it('should support timeouts whilst running an async function', async () => {
    const context = await createContext(logger, {timeout: 111})

    await Promise.all([
      expect(context.do(() => new Promise(() => {}))).rejects.toThrow(TimedOut),
      new Promise(resolve => {
        jest.runAllTimers()
        resolve()
      }),
    ])
  })

  it('should support running async functions when already timed out', async () => {
    const context = await createContext(logger, {timeout: 111})
    jest.runAllTimers()

    await expect(context.do(() => new Promise(() => {}))).rejects.toThrow(TimedOut)
  })

  it('should support timing out via a parent context', async () => {
    const context = await createContext(logger, {timeout: 111})
    const childContext = await createContext(logger, {context})

    await Promise.all([
      expect(childContext.do(() => new Promise(() => {}))).rejects.toThrow(TimedOut),
      new Promise(resolve => {
        jest.runAllTimers()
        resolve()
      }),
    ])
  })

  it('should support timing out of a child context', async () => {
    const context = await createContext(logger)
    const childContext = await createContext(logger, {context, timeout: 111})

    await Promise.all([
      expect(childContext.do(() => new Promise(() => {}))).rejects.toThrow(TimedOut),
      new Promise(resolve => {
        jest.runAllTimers()
        resolve()
      }),
    ])

    await expect(context.check()).resolves.toBeUndefined()
  })

  it('should remain canceled if timeout would have occurred later', async () => {
    const context = await createContext(logger, {timeout: 111})
    await context.cancel()

    await expect(context.check()).rejects.toThrow(Canceled)

    jest.runAllTimers()

    await expect(context.check()).rejects.toThrow(Canceled)
  })

  it('should remain timed out if subsequently canceled', async () => {
    const context = await createContext(logger, {timeout: 111})
    jest.runAllTimers()

    await expect(context.check()).rejects.toThrow(TimedOut)

    await context.cancel()

    await expect(context.check()).rejects.toThrow(TimedOut)
  })

  it('should support done handlers that resolve', async () => {
    const context = await createContext(logger)
    const doneHandler = jest.fn(async () => {})

    await context.onceDone(doneHandler)
    await context.cancel()

    expect(doneHandler).toHaveBeenCalledWith(expect.any(Canceled))
  })

  it('should support done handlers that reject', async () => {
    const context = await createContext(logger)
    const doneHandler = jest.fn(async () => { throw new Error('error-a') })

    await context.onceDone(doneHandler)

    await expect(context.cancel()).rejects.toThrow('error-a')
    expect(doneHandler).toHaveBeenCalledWith(expect.any(Canceled))
  })

  it('should call done handlers in reverse order', async () => {
    const context = await createContext(logger)
    const doneHandlerA = jest.fn(async () => {})
    const doneHandlerB = jest.fn(async () => {})

    await context.onceDone(doneHandlerA)
    await context.onceDone(doneHandlerB)
    await context.cancel()

    expect(doneHandlerA).toHaveBeenCalledWith(expect.any(Canceled))
    expect(doneHandlerB).toHaveBeenCalledWith(expect.any(Canceled))
    expect(doneHandlerA).toHaveBeenCalledAfter(doneHandlerB)
  })

  it('should immediately invoke done handlers when already done', async () => {
    const context = await createContext(logger)
    const doneHandler = jest.fn(async () => {})

    await context.cancel()

    await expect(context.onceDone(doneHandler)).rejects.toThrow(Canceled)
    expect(doneHandler).toHaveBeenCalledWith(expect.any(Canceled))
  })

  it('should wait for done handlers to complete when canceling', async () => {
    const context = await createContext(logger)

    let doneResolve
    const donePromise = new Promise(resolve => { doneResolve = jest.fn(resolve) })
    const doneHandler = () => donePromise
    await context.onceDone(doneHandler)

    const cancelResolved = jest.fn()

    await Promise.all([
      context.cancel().then(cancelResolved),
      new Promise(resolve => {
        doneResolve()
        resolve()
      }),
    ])

    expect(cancelResolved).toHaveBeenCalledAfter(doneResolve)
  })
})
