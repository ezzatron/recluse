module.exports = {
  createMockExit,
}

function createMockExit (logger) {
  if (!jest.isMockFunction(logger.error)) jest.spyOn(logger, 'error')

  return function exit (statusCode) {
    if (statusCode === 0) return

    const {calls} = logger.error.mock
    const lastCall = calls[calls.length - 1]

    if (lastCall) {
      throw new Error(`Process exited with status code ${statusCode} after logging error ${JSON.stringify(lastCall)}`)
    } else {
      throw new Error(`Process exited with status code ${statusCode}`)
    }
  }
}
