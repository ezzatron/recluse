module.exports = {
  createLazyGetter,
}

/**
 * Add a property to an object, with a value that is lazily determined.
 */
function createLazyGetter (object, name, factory) {
  let isCalled = false
  let value

  Object.defineProperty(object, name, {
    enumerable: true,

    get: () => {
      if (!isCalled) {
        value = factory()
        isCalled = true
      }

      return value
    },
  })
}
