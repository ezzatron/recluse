const {expect} = require('chai')
const {spy} = require('sinon')

const {createStateController} = require('../../src/state.js')
const {serialization: {copy}} = require('../../src/serialization/json.js')

describe('createStateController()', function () {
  const increment = async state => { state.number++ }
  const nest = async state => ({state})
  const destroy = async () => null
  const error = new Error('You done goofed')
  const explode = () => { throw error }

  beforeEach(function () {
    this.init = {number: 0}
    this.read = spy(async () => this.init)

    this.controller = createStateController(copy, this.read)
  })

  context('when the state has not been read', function () {
    it('should not be flagged as needing an update', function () {
      expect(this.controller.isUpdated()).to.be.false()
    })

    describe('getState()', function () {
      it('should be able to return the state', async function () {
        expect(await this.controller.readState()).to.deep.equal(this.init)
      })
    })

    describe('readState()', function () {
      it('should be able to read the state', async function () {
        expect(await this.controller.readState()).to.deep.equal(this.init)
      })

      it('should return a copy of the state', async function () {
        expect(await this.controller.readState()).to.not.equal(this.init)
      })
    })

    describe('updateState()', function () {
      it('should be able to update the state by mutation', async function () {
        await this.controller.updateState(increment)

        expect(await this.controller.readState()).to.deep.equal({number: 1})
      })

      it('should be able to replace the state by supplying a new state', async function () {
        await this.controller.updateState({x: 'y'})

        expect(await this.controller.readState()).to.deep.equal({x: 'y'})
      })

      it('should be able to replace the state with null by supplying null', async function () {
        await this.controller.updateState(null)

        expect(await this.controller.readState()).to.be.null()
      })

      it('should be able to replace the state by returning a new state', async function () {
        await this.controller.updateState(nest)

        expect(await this.controller.readState()).to.deep.equal({state: this.init})
      })

      it('should be able to replace the state with null by returning null', async function () {
        await this.controller.updateState(destroy)

        expect(await this.controller.readState()).to.be.null()
      })

      it('should not update the state if the update operation throws', async function () {
        await expect(this.controller.updateState(explode)).to.be.rejectedWith(error)
        expect(await this.controller.readState()).to.deep.equal(this.init)
      })
    })
  })

  context('when the state has been read', function () {
    beforeEach(async function () {
      this.state = await this.controller.readState()
    })

    it('should not be flagged as needing an update', function () {
      expect(this.controller.isUpdated()).to.be.false()
    })

    describe('getState()', function () {
      it('should be able to return the state', async function () {
        expect(await this.controller.readState()).to.deep.equal(this.init)
      })

      it('should not cause additional calls of the read callback', async function () {
        await this.controller.getState()

        expect(this.read).to.have.been.calledOnce()
      })
    })

    describe('readState()', function () {
      it('should be able to read the state', async function () {
        expect(await this.controller.readState()).to.deep.equal(this.init)
      })

      it('should return a copy of the state', async function () {
        expect(await this.controller.readState()).to.not.equal(this.init)
        expect(await this.controller.readState()).to.not.equal(this.state)
      })

      it('should not cause additional calls of the read callback', async function () {
        await this.controller.readState()

        expect(this.read).to.have.been.calledOnce()
      })
    })

    describe('updateState()', function () {
      it('should be able to update the state by mutation', async function () {
        await this.controller.updateState(increment)

        expect(await this.controller.readState()).to.deep.equal({number: 1})
      })

      it('should be able to replace the state by supplying a new state', async function () {
        await this.controller.updateState({x: 'y'})

        expect(await this.controller.readState()).to.deep.equal({x: 'y'})
      })

      it('should be able to replace the state with null by supplying null', async function () {
        await this.controller.updateState(null)

        expect(await this.controller.readState()).to.be.null()
      })

      it('should be able to replace the state by returning a new state', async function () {
        await this.controller.updateState(nest)

        expect(await this.controller.readState()).to.deep.equal({state: this.init})
      })

      it('should be able to replace the state with null by returning null', async function () {
        await this.controller.updateState(destroy)

        expect(await this.controller.readState()).to.be.null()
      })

      it('should not cause additional calls of the read callback', async function () {
        await this.controller.updateState(null)

        expect(this.read).to.have.been.calledOnce()
      })

      it('should not update the state if the update operation throws', async function () {
        await expect(this.controller.updateState(explode)).to.be.rejectedWith(error)
        expect(await this.controller.readState()).to.deep.equal(this.init)
      })
    })
  })

  context('when the state has been updated', function () {
    beforeEach(async function () {
      await this.controller.updateState(increment)
      this.state = await this.controller.readState()
    })

    it('should be flagged as needing an update', function () {
      expect(this.controller.isUpdated()).to.be.true()
    })

    describe('getState()', function () {
      it('should be able to return the state', async function () {
        expect(await this.controller.readState()).to.deep.equal(this.state)
      })

      it('should not cause additional calls of the read callback', async function () {
        await this.controller.getState()

        expect(this.read).to.have.been.calledOnce()
      })
    })

    describe('readState()', function () {
      it('should be able to read the state', async function () {
        expect(await this.controller.readState()).to.deep.equal(this.state)
      })

      it('should return a copy of the state', async function () {
        expect(await this.controller.readState()).to.not.equal(this.state)
      })

      it('should not cause additional calls of the read callback', async function () {
        await this.controller.readState()

        expect(this.read).to.have.been.calledOnce()
      })
    })

    describe('updateState()', function () {
      it('should be able to update the state by mutation', async function () {
        await this.controller.updateState(increment)

        expect(await this.controller.readState()).to.deep.equal({number: 2})
      })

      it('should be able to replace the state by supplying a new state', async function () {
        await this.controller.updateState({x: 'y'})

        expect(await this.controller.readState()).to.deep.equal({x: 'y'})
      })

      it('should be able to replace the state with null by supplying null', async function () {
        await this.controller.updateState(null)

        expect(await this.controller.readState()).to.be.null()
      })

      it('should be able to replace the state by returning a new state', async function () {
        await this.controller.updateState(nest)

        expect(await this.controller.readState()).to.deep.equal({state: this.state})
      })

      it('should be able to replace the state with null by returning null', async function () {
        await this.controller.updateState(destroy)

        expect(await this.controller.readState()).to.be.null()
      })

      it('should not cause additional calls of the read callback', async function () {
        await this.controller.updateState(null)

        expect(this.read).to.have.been.calledOnce()
      })

      it('should not update the state if the update operation throws', async function () {
        await expect(this.controller.updateState(explode)).to.be.rejectedWith(error)
        expect(await this.controller.readState()).to.deep.equal(this.state)
      })
    })
  })

  context('when the state has been updated by mutation', function () {
    beforeEach(async function () {
      await this.controller.updateState(state => {
        ++state.number
        this.draftState = state
      })
    })

    it('should copy the supplied state to prevent mutations', async function () {
      this.draftState.number = 'other'

      expect(await this.controller.readState()).to.deep.equal({number: 1})
    })
  })

  context('when the state has been updated by returning a new state', function () {
    beforeEach(async function () {
      this.state = {number: 111}
      await this.controller.updateState(() => this.state)
    })

    it('should copy the supplied state to prevent mutations', async function () {
      this.state.number = 222

      expect(await this.controller.readState()).to.deep.equal({number: 111})
    })
  })

  context('when the state has been updated by supplying a new state', function () {
    beforeEach(async function () {
      this.state = {number: 111}
      await this.controller.updateState(this.state)
    })

    it('should copy the supplied state to prevent mutations', async function () {
      this.state.number = 222

      expect(await this.controller.readState()).to.deep.equal({number: 111})
    })
  })
})
