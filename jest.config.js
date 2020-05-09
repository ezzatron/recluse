module.exports = {
  collectCoverageFrom: [
    'src/**/*.js',
  ],
  coverageDirectory: 'artifacts/coverage/jest',
  restoreMocks: true,
  setupFilesAfterEnv: [
    'jest-extended',
  ],
  testEnvironment: 'node',
  testMatch: [
    '**/test/suite/**/*.spec.js',
  ],
}
