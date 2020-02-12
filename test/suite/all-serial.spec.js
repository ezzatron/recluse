const {allSerial} = require('../../src/async.js')

describe('allSerial()', () => {
  const wait = millis => new Promise(resolve => setTimeout(resolve, millis))

  describe('in all circumstances', () => {
    it('should execute the supplied functions serially', async () => {
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
        },
      )

      expect(events).toEqual([
        'a start',
        'a end',
        'b start',
        'b end',
      ])
    })
  })

  describe('when none of the supplied functions produce errors', () => {
    it('should return an array of return values', async () => {
      const result = await allSerial(
        async () => 'a',
        async () => 'b',
      )

      expect(result).toEqual(['a', 'b'])
    })
  })

  describe('when some of the supplied functions produce errors', () => {
    it('should produce a wrapper error', async () => {
      const operation = allSerial(
        async () => {},
        async () => { throw new Error() },
      )

      await expect(operation).rejects.toThrow()
    })

    it('should generate an error message that includes the produced error messages', async () => {
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

      expect(error.message).toBe([
        'Error(s) occurred:',
        '  - a',
        '  - b',
      ].join('\n'))
    })

    it('should store the original errors in a property on the wrapper error', async () => {
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

      expect(error.errors).toBeArrayOfSize(2)
      expect(error.errors[0]).toBe(errorA)
      expect(error.errors[1]).toBe(errorB)
    })
  })
})
