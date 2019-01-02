const Docker = require('dockerode')
const {Client} = require('pg')
const {expect} = require('chai')

const {initializeSchema} = require('../../src/index.js')

describe('Events', function () {
  before(async function () {
    this.timeout(30 * 1000)

    const docker = new Docker()

    this.postgresContainer = await docker.createContainer({
      name: `test-postgres-${Math.random().toString(36).substring(2, 15)}`,
      Image: 'postgres:alpine',
      Env: [
        'POSTGRES_USER=user',
        'POSTGRES_PASSWORD=password',
        'POSTGRES_DB=test',
      ],
      PublishAllPorts: true,
    })
    await this.postgresContainer.start()

    const inspection = await this.postgresContainer.inspect()
    const attempts = 10

    for (let i = 0; i < attempts; ++i) {
      try {
        this.pgClient = new Client({
          host: 'localhost',
          port: inspection.NetworkSettings.Ports['5432/tcp'][0].HostPort,
          user: 'user',
          password: 'password',
          database: 'test',
        })
        await this.pgClient.connect()

        break
      } catch (error) {
        if (i === attempts - 1) throw error
      }

      await new Promise(resolve => setTimeout(resolve, 500))
    }
  })

  describe('initializeSchema()', function () {
    it('should create the necessary tables', async function () {
      await initializeSchema(this.pgClient)
      const actual = await this.pgClient.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
      )

      expect(actual.rows).to.deep.include({table_name: 'global_offset'})
      expect(actual.rows).to.deep.include({table_name: 'stream'})
      expect(actual.rows).to.deep.include({table_name: 'event'})
    })
  })

  after(async function () {
    this.timeout(10 * 1000)

    try {
      await this.pgClient.end()
    } catch (error) {}

    await this.postgresContainer.remove({force: true, v: true})
  })
})
