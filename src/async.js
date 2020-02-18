class CancelError extends Error {
  constructor () {
    super('Cancelled')
  }
}

class TimeoutError extends Error {
  constructor () {
    super('Timeout')
  }
}

module.exports = {
  CancelError,
  createCancelController,
  createTimeout,
  TimeoutError,
}

function createCancelController () {
  let isCancelled = false
  let rejectPromise

  const controller = new Promise((resolve, reject) => { rejectPromise = reject })
  controller.catch(() => {})

  Object.defineProperty(controller, 'isCancelled', {
    get () {
      return isCancelled
    },
  })

  controller.cancel = function cancel () {
    if (isCancelled) return

    isCancelled = true
    rejectPromise(new CancelError())
  }

  return controller
}

function createTimeout (clock, delay) {
  return new Promise((resolve, reject) => {
    clock.setTimeout(() => {
      reject(new TimeoutError())
    }, delay)
  })
}
