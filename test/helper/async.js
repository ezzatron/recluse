module.exports = {
  asyncIterableToArray,
  consumeAsyncIterable,
}

async function asyncIterableToArray (iterable) {
  if (iterable == null) throw new Error('Not an object')

  const iteratorFactory = iterable[Symbol.asyncIterator]

  if (typeof iteratorFactory !== 'function') throw new Error('Not an async iterable')

  const iterator = iteratorFactory()
  const array = []
  let returnValue
  let value, done

  do {
    ({value, done} = await iterator.next())

    if (done) {
      returnValue = value
    } else {
      array.push(value)
    }
  } while (!done)

  return [array, returnValue]
}

async function consumeAsyncIterable (iterable, count, onDone, onIteration) {
  if (iterable == null) throw new Error('Not an object')

  const iteratorFactory = iterable[Symbol.asyncIterator]

  if (typeof iteratorFactory !== 'function') throw new Error('Not an async iterable')

  const iterator = iteratorFactory()

  for (let i = 0; i < count; ++i) {
    const {done, value} = await iterator.next()

    if (done) throw new Error('Unexpected end of async iterable')

    if (onIteration) await onIteration(value, i)
  }

  if (onDone) await onDone(iterable)
}
