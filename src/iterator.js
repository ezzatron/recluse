module.exports = {
  acquireAsyncIterator,
}

/**
 * Return an async iterator for the supplied async iterable.
 */
function acquireAsyncIterator (asyncIterable) {
  const iteratorFactory = asyncIterable[Symbol.asyncIterator]

  if (typeof iteratorFactory !== 'function') throw new Error('Supplied value is not an async iterable')

  return iteratorFactory()
}
