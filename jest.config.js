module.exports = {
  testEnvironment: 'node',
  testMatch: [
    '**/test/suite/**/*.spec.js',
  ],
  setupFilesAfterEnv: [
    'jest-extended',
  ],
  collectCoverageFrom: [
    'src/**/*.js',
  ],
  coverageDirectory: 'artifacts/coverage/jest',
  restoreMocks: true,
}
