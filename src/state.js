module.exports = {
  createStateController,
}

function createStateController (serialization, read) {
  const {copy} = serialization
  let isReadFlag = false
  let isUpdatedFlag = false
  let state = null

  async function getState () {
    if (!isReadFlag) {
      state = await read()
      isReadFlag = true
    }

    return state
  }

  function isUpdated () {
    return isUpdatedFlag
  }

  async function readState () {
    return copy(await getState())
  }

  async function updateState (produce) {
    if (typeof produce !== 'function') {
      state = copy(produce)
      isReadFlag = true
      isUpdatedFlag = true

      return
    }

    const draft = await readState()
    const result = await produce(draft)

    state = copy(typeof result === 'undefined' ? draft : result)
    isUpdatedFlag = true
  }

  return {
    getState,
    isUpdated,
    readState,
    updateState,
  }
}
