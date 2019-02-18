module.exports = {
  allSerial,
}

/**
 * Execute all the supplied functions in the supplied order.
 *
 * This function is primarily used for executing cleanup tasks.
 *
 * If more than one function throws, a special wrapper exception will be thrown.
 * If only one function throws, that exception will be re-thrown. If all
 * functions return successfully, the return values of each function will be
 * returned as an array.
 */
async function allSerial (...fns) {
  const errors = []
  const results = []

  for (const fn of fns) {
    try {
      results.push(await fn())
    } catch (error) {
      errors.push(error)
    }
  }

  if (errors.length < 1) return results
  if (errors.length < 2) throw errors[0]

  const messageList = errors.map(({message}) => `\n  - ${message}`)
  const error = new Error(`Multple errors occurred:${messageList}`)
  error.errors = errors

  throw error
}
