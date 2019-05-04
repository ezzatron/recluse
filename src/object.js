module.exports = {
  createLazyGetter,
  mapObjectValues,
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

function mapObjectValues (fn, object) {
  const result = {}

  for (const key in object) {
    result[key] = fn(object[key], key)
  }

  return result
}
