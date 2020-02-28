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

  it('should reject with the last error encountered when running deferred functions', async () => {
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

    await expect(task).rejects.toThrow('error-a')
    expect(log).toEqual(['b', 'a'])
  })

  it('should allow deferreds to recover from errors', async () => {
    const log = []

    await withDefer(defer => {
      defer(recover => {
        log.push(recover().message)
      })

      defer(recover => {
        log.push(recover().message)

        throw new Error('error-a')
      })

      defer(recover => {
        log.push(recover().message)

        throw new Error('error-b')
      })

      throw new Error('error-c')
    })

    expect(log).toEqual(['error-c', 'error-b', 'error-a'])
  })

  it('should allow deferreds to re-throw errors', async () => {
    const log = []

    const task = withDefer(defer => {
      defer(recover => {
        const error = recover()
        log.push(`defer-a ${error.message}`)

        throw error
      })

      defer(recover => {
        const error = recover()
        log.push(`defer-b ${error.message}`)

        throw error
      })

      throw new Error('error-a')
    })

    await expect(task).rejects.toThrow('error-a')
    expect(log).toEqual(['defer-b error-a', 'defer-a error-a'])
  })

  it('should return nothing from recover() when no error has occurred', async () => {
    const log = []

    await withDefer(defer => {
      defer(recover => {
        log.push(`defer-a ${typeof recover()}`)
      })

      defer(recover => {
        log.push(`defer-b ${typeof recover()}`)
      })
    })

    expect(log).toEqual(['defer-b undefined', 'defer-a undefined'])
  })

  it('should return nothing from recover() once previous errors are recovered', async () => {
    const log = []

    await withDefer(defer => {
      defer(recover => {
        log.push(`defer-a ${typeof recover()}`)
      })

      defer(recover => {
        log.push(`defer-b ${typeof recover()}`)
      })

      throw new Error('error-a')
    })

    expect(log).toEqual(['defer-b object', 'defer-a undefined'])
  })
})
