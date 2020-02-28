const {withDefer} = require('../../../src/async.js')

describe('withDefer()', () => {
  it('should support async and sync functions', async () => {
    await expect(withDefer(async () => 'value-a')).resolves.toBe('value-a')
    await expect(withDefer(() => 'value-a')).resolves.toBe('value-a')
    await expect(withDefer(async () => { throw new Error('error-a') })).rejects.toThrow('error-a')
    await expect(withDefer(() => { throw new Error('error-a') })).rejects.toThrow('error-a')
  })

  it('should execute async deferred functions in reverse order of defer calls', async () => {
    const log = []

    await withDefer(defer => {
      log.push('a')
      defer(async () => { log.push('b') })
      defer(async () => { log.push('c') })
      log.push('d')
    })

    expect(log).toEqual(['a', 'd', 'c', 'b'])
  })

  it('should execute sync deferred functions in reverse order of defer calls', async () => {
    const log = []

    await withDefer(defer => {
      log.push('a')
      defer(() => { log.push('b') })
      defer(() => { log.push('c') })
      log.push('d')
    })

    expect(log).toEqual(['a', 'd', 'c', 'b'])
  })

  it('should execute mixed async and sync deferred functions in reverse order of defer calls', async () => {
    const log = []

    await withDefer(defer => {
      log.push('a')
      defer(() => { log.push('b') })
      defer(async () => { log.push('c') })
      log.push('d')
    })

    expect(log).toEqual(['a', 'd', 'c', 'b'])
  })

  it('should execute deferred functions when an async main work function rejects', async () => {
    const log = []

    const task = withDefer(async defer => {
      log.push('a')
      defer(() => { log.push('b') })
      defer(() => { log.push('c') })
      throw new Error('error-a')
    })

    await expect(task).rejects.toThrow('error-a')
    expect(log).toEqual(['a', 'c', 'b'])
  })

  it('should execute deferred functions when a sync main work function throws', async () => {
    const log = []

    const task = withDefer(defer => {
      log.push('a')
      defer(() => { log.push('b') })
      defer(() => { log.push('c') })
      throw new Error('error-a')
    })

    await expect(task).rejects.toThrow('error-a')
    expect(log).toEqual(['a', 'c', 'b'])
  })

  it('should reject with the first error encountered when running deferred functions', async () => {
    const log = []

    const task = withDefer(defer => {
      defer(() => {
        log.push('a')
        throw new Error('error-a')
      })

      defer(() => {
        log.push('b')
        throw new Error('error-b')
      })
    })

    await expect(task).rejects.toThrow('error-b')
    expect(log).toEqual(['b', 'a'])
  })

  it('should reject with the main function error when running deferred functions that throw', async () => {
    const log = []

    const task = withDefer(defer => {
      defer(() => {
        log.push('a')
        throw new Error('error-a')
      })

      defer(() => {
        log.push('b')
        throw new Error('error-b')
      })

      throw new Error('error-c')
    })

    await expect(task).rejects.toThrow('error-c')
    expect(log).toEqual(['b', 'a'])
  })

  it('should allow deferreds to recover from errors', async () => {
    const log = []

    await withDefer(defer => {
      defer(async recover => {
        await recover(async error => { log.push(error.message) })
      })

      defer(async recover => {
        await recover(async error => { log.push(error.message) })

        throw new Error('error-a')
      })

      defer(async recover => {
        await recover(async error => { log.push(error.message) })

        throw new Error('error-b')
      })

      throw new Error('error-c')
    })

    expect(log).toEqual(['error-c', 'error-b', 'error-a'])
  })

  it('should allow deferreds to re-throw errors', async () => {
    const log = []

    const task = withDefer(defer => {
      defer(async recover => {
        await recover(async error => {
          log.push(error.message)

          throw error
        })
      })

      defer(async recover => {
        await recover(async error => {
          log.push(error.message)

          throw error
        })
      })

      throw new Error('error-a')
    })

    await expect(task).rejects.toThrow('error-a')
    expect(log).toEqual(['error-a', 'error-a'])
  })
})
