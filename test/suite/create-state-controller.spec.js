const {createStateController} = require('../../src/state.js')
const {serialization: {copy}} = require('../../src/serialization/json.js')

describe('createStateController()', () => {
  const increment = async state => { state.number++ }
  const nest = async state => ({state})
  const destroy = async () => null
  const error = new Error('You done goofed')
  const explode = () => { throw error }

  let controller, draftState, init, read, state

  beforeEach(() => {
    init = {number: 0}
    read = jest.fn(async () => init)

    controller = createStateController(copy, read)
  })

  describe('when the state has not been read', () => {
    it('should not be flagged as needing an update', () => {
      expect(controller.isUpdated()).toBe(false)
    })

    describe('getState()', () => {
      it('should be able to return the state', async () => {
        expect(await controller.readState()).toEqual(init)
      })
    })

    describe('readState()', () => {
      it('should be able to read the state', async () => {
        expect(await controller.readState()).toEqual(init)
      })

      it('should return a copy of the state', async () => {
        expect(await controller.readState()).not.toBe(init)
      })
    })

    describe('updateState()', () => {
      it('should be able to update the state by mutation', async () => {
        await controller.updateState(increment)

        expect(await controller.readState()).toEqual({number: 1})
      })

      it('should be able to replace the state by supplying a new state', async () => {
        await controller.updateState({x: 'y'})

        expect(await controller.readState()).toEqual({x: 'y'})
      })

      it('should be able to replace the state with null by supplying null', async () => {
        await controller.updateState(null)

        expect(await controller.readState()).toBeNull()
      })

      it('should be able to replace the state by returning a new state', async () => {
        await controller.updateState(nest)

        expect(await controller.readState()).toEqual({state: init})
      })

      it('should be able to replace the state with null by returning null', async () => {
        await controller.updateState(destroy)

        expect(await controller.readState()).toBeNull()
      })

      it('should not update the state if the update operation throws', async () => {
        await expect(controller.updateState(explode)).rejects.toThrow(error)
        expect(await controller.readState()).toEqual(init)
      })
    })
  })

  describe('when the state has been read', () => {
    beforeEach(async () => {
      state = await controller.readState()
    })

    it('should not be flagged as needing an update', () => {
      expect(controller.isUpdated()).toBe(false)
    })

    describe('getState()', () => {
      it('should be able to return the state', async () => {
        expect(await controller.readState()).toEqual(init)
      })

      it('should not cause additional calls of the read callback', async () => {
        await controller.getState()

        expect(read).toHaveBeenCalledTimes(1)
      })
    })

    describe('readState()', () => {
      it('should be able to read the state', async () => {
        expect(await controller.readState()).toEqual(init)
      })

      it('should return a copy of the state', async () => {
        expect(await controller.readState()).not.toBe(init)
        expect(await controller.readState()).not.toBe(state)
      })

      it('should not cause additional calls of the read callback', async () => {
        await controller.readState()

        expect(read).toHaveBeenCalledTimes(1)
      })
    })

    describe('updateState()', () => {
      it('should be able to update the state by mutation', async () => {
        await controller.updateState(increment)

        expect(await controller.readState()).toEqual({number: 1})
      })

      it('should be able to replace the state by supplying a new state', async () => {
        await controller.updateState({x: 'y'})

        expect(await controller.readState()).toEqual({x: 'y'})
      })

      it('should be able to replace the state with null by supplying null', async () => {
        await controller.updateState(null)

        expect(await controller.readState()).toBeNull()
      })

      it('should be able to replace the state by returning a new state', async () => {
        await controller.updateState(nest)

        expect(await controller.readState()).toEqual({state: init})
      })

      it('should be able to replace the state with null by returning null', async () => {
        await controller.updateState(destroy)

        expect(await controller.readState()).toBeNull()
      })

      it('should not cause additional calls of the read callback', async () => {
        await controller.updateState(null)

        expect(read).toHaveBeenCalledTimes(1)
      })

      it('should not update the state if the update operation throws', async () => {
        await expect(controller.updateState(explode)).rejects.toThrow(error)
        expect(await controller.readState()).toEqual(init)
      })
    })
  })

  describe('when the state has been updated', () => {
    beforeEach(async () => {
      await controller.updateState(increment)
      state = await controller.readState()
    })

    it('should be flagged as needing an update', () => {
      expect(controller.isUpdated()).toBe(true)
    })

    describe('getState()', () => {
      it('should be able to return the state', async () => {
        expect(await controller.readState()).toEqual(state)
      })

      it('should not cause additional calls of the read callback', async () => {
        await controller.getState()

        expect(read).toHaveBeenCalledTimes(1)
      })
    })

    describe('readState()', () => {
      it('should be able to read the state', async () => {
        expect(await controller.readState()).toEqual(state)
      })

      it('should return a copy of the state', async () => {
        expect(await controller.readState()).not.toBe(state)
      })

      it('should not cause additional calls of the read callback', async () => {
        await controller.readState()

        expect(read).toHaveBeenCalledTimes(1)
      })
    })

    describe('updateState()', () => {
      it('should be able to update the state by mutation', async () => {
        await controller.updateState(increment)

        expect(await controller.readState()).toEqual({number: 2})
      })

      it('should be able to replace the state by supplying a new state', async () => {
        await controller.updateState({x: 'y'})

        expect(await controller.readState()).toEqual({x: 'y'})
      })

      it('should be able to replace the state with null by supplying null', async () => {
        await controller.updateState(null)

        expect(await controller.readState()).toBeNull()
      })

      it('should be able to replace the state by returning a new state', async () => {
        await controller.updateState(nest)

        expect(await controller.readState()).toEqual({state})
      })

      it('should be able to replace the state with null by returning null', async () => {
        await controller.updateState(destroy)

        expect(await controller.readState()).toBeNull()
      })

      it('should not cause additional calls of the read callback', async () => {
        await controller.updateState(null)

        expect(read).toHaveBeenCalledTimes(1)
      })

      it('should not update the state if the update operation throws', async () => {
        await expect(controller.updateState(explode)).rejects.toThrow(error)
        expect(await controller.readState()).toEqual(state)
      })
    })
  })

  describe('when the state has been updated by mutation', () => {
    beforeEach(async () => {
      await controller.updateState(state => {
        ++state.number
        draftState = state
      })
    })

    it('should copy the supplied state to prevent mutations', async () => {
      draftState.number = 'other'

      expect(await controller.readState()).toEqual({number: 1})
    })
  })

  describe('when the state has been updated by returning a new state', () => {
    beforeEach(async () => {
      state = {number: 111}
      await controller.updateState(() => state)
    })

    it('should copy the supplied state to prevent mutations', async () => {
      state.number = 222

      expect(await controller.readState()).toEqual({number: 111})
    })
  })

  describe('when the state has been updated by supplying a new state', () => {
    beforeEach(async () => {
      state = {number: 111}
      await controller.updateState(state)
    })

    it('should copy the supplied state to prevent mutations', async () => {
      state.number = 222

      expect(await controller.readState()).toEqual({number: 111})
    })
  })
})
