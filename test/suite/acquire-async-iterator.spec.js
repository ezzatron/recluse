const {expect} = require('chai')

const {acquireAsyncIterator} = require('../../src/iterator.js')

describe('acquireAsyncIterator()', function () {
  it('should return the correct async iterator when supplied with an async iterable', async function () {
    const iterable = await (async function * () {})()
    const iteratorFactory = iterable[Symbol.asyncIterator]

    expect(acquireAsyncIterator(iterable)).to.equal(iteratorFactory())
  })

  it('should throw when supplied an object that is not an async iterable', async function () {
    expect(() => acquireAsyncIterator({})).to.throw('Supplied value is not an async iterable')
  })

  it('should throw when supplied an value that is not an object', async function () {
    expect(() => acquireAsyncIterator(null)).to.throw('Supplied value is not an async iterable')
    expect(() => acquireAsyncIterator()).to.throw('Supplied value is not an async iterable')
  })
})
