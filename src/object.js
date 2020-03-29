module.exports = {
  createLazyGetter,
}

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
