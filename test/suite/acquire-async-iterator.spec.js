const {acquireAsyncIterator} = require('../../src/iterator.js')

describe('acquireAsyncIterator()', () => {
  it('should return the correct async iterator when supplied with an async iterable', async () => {
    const iterable = await (async function * () {})()
    const iteratorFactory = iterable[Symbol.asyncIterator]

    expect(acquireAsyncIterator(iterable)).toBe(iteratorFactory())
  })

  it('should throw when supplied an object that is not an async iterable', async () => {
    expect(() => acquireAsyncIterator({})).toThrow('Supplied value is not an async iterable')
  })

  it('should throw when supplied an value that is not an object', async () => {
    expect(() => acquireAsyncIterator(null)).toThrow('Supplied value is not an async iterable')
    expect(() => acquireAsyncIterator()).toThrow('Supplied value is not an async iterable')
  })
})
