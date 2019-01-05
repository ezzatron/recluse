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
}
