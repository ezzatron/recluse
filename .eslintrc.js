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
    'arrow-parens': ['error', 'as-needed'],
    'comma-dangle': ['error', {
      arrays: 'always-multiline',
      objects: 'always-multiline',
      imports: 'always-multiline',
      exports: 'always-multiline',
      functions: 'always-multiline',
    }],
    'no-prototype-builtins': 'off',
    'no-unused-vars': ['error', {
      args: 'after-used',
      ignoreRestSiblings: true,
    }],
    'object-curly-spacing': ['error', 'never'],
    'padding-line-between-statements': ['error', {
      blankLine: 'always',
      prev: '*',
      next: 'return',
    }],
    'prefer-const': 'error',
    'quote-props': ['error', 'as-needed'],

    'import/default': 'error',
    'import/dynamic-import-chunkname': 'error',
    'import/export': 'error',
    'import/extensions': ['error', 'ignorePackages'],
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
    'import/order': ['error', {
      alphabetize: {order: 'asc'},
      groups: [
        ['builtin', 'external'],
        ['index', 'internal', 'parent', 'sibling', 'unknown'],
      ],
      'newlines-between': 'ignore',
      pathGroups: [
        {pattern: '~/**', group: 'internal'},
      ],
    }],

    'no-alert': 'warn',
    'no-console': 'warn',
    'no-debugger': 'warn',

    'jest/no-focused-tests': 'warn',
  }
}
