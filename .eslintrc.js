module.exports = {
  extends: [
    'standard',
    'plugin:jest/recommended',
    'plugin:jest/style',
  ],
  plugins: [
    'jest'
  ],
  env: {
    jest: true
  },
  rules: {
    'comma-dangle': ['error', 'always-multiline'],
    'no-unused-vars': 'error',
    'object-curly-spacing': ['error', 'never'],
    'prefer-const': 'error',
    'quote-props': ['error', 'as-needed'],

    'import/default': 'error',
    'import/export': 'error',
    'import/extensions': ['error', 'always'],
    'import/first': 'error',
    'import/named': 'error',
    'import/namespace': 'error',
    'import/newline-after-import': 'error',
    'import/no-amd': 'error',
    'import/no-cycle': 'error',
    'import/no-deprecated': 'error',
    'import/no-duplicates': 'error',
    'import/no-extraneous-dependencies': 'error',
    'import/no-mutable-exports': 'error',
    'import/no-named-as-default-member': 'error',
    'import/no-named-default': 'error',
    'import/no-self-import': 'error',
    'import/no-unresolved': 'error',
    'import/no-useless-path-segments': 'error',

    'jest/no-focused-tests': 'warn',
  }
}
