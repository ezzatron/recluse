const chai = require('chai')
const chaiAsPromised = require('chai-as-promised')
const chaiBytes = require('chai-bytes')
const dirtyChai = require('dirty-chai')
const sinonChai = require('sinon-chai')

const chaiPg = require('./chai-pg.js')

chai.use(chaiBytes)
chai.use(chaiPg)
chai.use(dirtyChai)
chai.use(sinonChai)
chai.use(chaiAsPromised)
