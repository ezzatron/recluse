module.exports = {
  createClock,
}

function createClock (options = {}) {
  const {immediate} = options

  const timeouts = {}
  let timeoutId = 1

  return {
    setTimeout (fn) {
      const id = timeoutId++

      if (!immediate) {
        timeouts[id] = fn

        return id
      }

      try {
        fn()
      } catch (e) {}

      return id
    },

    clearTimeout (id) {
      delete timeouts[id]
    },

    runAll () {
      for (const id of Object.keys(timeouts)) {
        try {
          timeouts[id]()
        } catch (e) {}

        delete timeouts[id]
      }
    },
  }
}
