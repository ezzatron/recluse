const {expect} = require('chai')

const {allSerial} = require('../../src/async.js')

describe('allSerial()', function () {
  const wait = millis => new Promise(resolve => setTimeout(resolve, millis))

  context('in all circumstances', function () {
    it('should execute the supplied functions serially', async function () {
      const events = []
      await allSerial(
        async () => {
          events.push('a start')
          await wait(10)
          events.push('a end')
        },
        async () => {
          events.push('b start')
          await wait(0)
          events.push('b end')
        }
      )

      expect(events).to.deep.equal([
        'a start',
        'a end',
        'b start',
        'b end',
      ])
    })
  })

  context('when none of the supplied functions produce errors', function () {
    it('should return an array of return values', async function () {
      const result = await allSerial(
        async () => 'a',
        async () => 'b'
      )

      expect(result).to.deep.equal(['a', 'b'])
    })
  })

  context('when some of the supplied functions produce errors', function () {
    it('should produce a wrapper error', async function () {
      const operation = allSerial(
        async () => {},
        async () => { throw new Error() },
      )

      await expect(operation).to.be.rejected()
    })

    it('should generate an error message that includes the produced error messages', async function () {
      const errorA = new Error('a')
      const errorB = new Error('b')
      let error
      try {
        await allSerial(
          async () => { throw errorA },
          async () => {},
          async () => { throw errorB },
        )
      } catch (e) {
        error = e
      }

      expect(error.message).to.equal([
        'Error(s) occurred:',
        '  - a',
        '  - b',
      ].join('\n'))
    })

    it('should store the original errors in a property on the wrapper error', async function () {
      const errorA = new Error('a')
      const errorB = new Error('b')
      let error
      try {
        await allSerial(
          async () => { throw errorA },
          async () => {},
          async () => { throw errorB },
        )
      } catch (e) {
        error = e
      }

      expect(error.errors).to.be.an('array')
      expect(error.errors).to.have.length(2)
      expect(error.errors[0]).to.equal(errorA)
      expect(error.errors[1]).to.equal(errorB)
    })
  })
})
