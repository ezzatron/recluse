const chai = require('chai')
const chaiBytes = require('chai-bytes')
const dirtyChai = require('dirty-chai')

const chaiPg = require('./chai-pg.js')

chai.use(chaiBytes)
chai.use(chaiPg)
chai.use(dirtyChai)
