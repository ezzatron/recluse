const {expect} = require('chai')
const {pgSpec} = require('../helper.js')

const {initializeSchema} = require('../../src/index.js')

describe('initializeSchema()', pgSpec(function () {
  const selectTable = "SELECT * FROM information_schema.tables WHERE table_schema = 'recluse' AND table_name = $1"

  it('should create the necessary tables', async function () {
    await initializeSchema(this.pgClient)

    expect(await this.query(selectTable, ['global_offset'])).to.have.rowCount(1)
    expect(await this.query(selectTable, ['stream'])).to.have.rowCount(1)
    expect(await this.query(selectTable, ['event'])).to.have.rowCount(1)
  })

  it('should be safe to run multiple times', async function () {
    await initializeSchema(this.pgClient)
    await initializeSchema(this.pgClient)

    expect(await this.query(selectTable, ['global_offset'])).to.have.rowCount(1)
    expect(await this.query(selectTable, ['stream'])).to.have.rowCount(1)
    expect(await this.query(selectTable, ['event'])).to.have.rowCount(1)
  })
}))
