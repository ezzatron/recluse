const chai = require('chai')
const chaiBytes = require('chai-bytes')

const chaiPg = require('./chai-pg.js')

chai.use(chaiBytes)
chai.use(chaiPg)
