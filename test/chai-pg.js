module.exports = (chai, utils) => {
  const {Assertion} = chai

  Assertion.addMethod('rowCount', function (count) {
    const subject = this._obj

    new Assertion(subject).to.have.property('rowCount')

    this.assert(
      subject.rowCount === count,
      'expected query to return #{exp} rows, but got #{act}',
      'expected query to not return #{act} rows',
      count,
      subject.rowCount
    )
  })

  Assertion.addMethod('row', function (fields) {
    const subject = this._obj

    new Assertion(subject).to.have.rowCount(1)
    new Assertion(subject).to.have.property('rows')

    const [row] = subject.rows

    new Assertion(row).to.have.fields(fields)
  })

  Assertion.addMethod('fields', function (fields) {
    const subject = this._obj

    new Assertion(subject).to.be.an('object')
    new Assertion(subject).to.not.be.null()

    for (const field in fields) {
      const actual = subject[field]
      const expected = fields[field]
      const type = typeof expected

      if (type === 'function') {
        new Assertion(actual).to.be.an.instanceOf(expected)
      } else if (Buffer.isBuffer(expected)) {
        new Assertion(actual, `expected ${field} bytes to match`)
          .to.equalBytes(expected)
      } else {
        new Assertion(actual, `expected ${field} to be ${JSON.stringify(expected)}, but got ${JSON.stringify(actual)}`)
          .to.equal(expected)
      }
    }
  })
}
