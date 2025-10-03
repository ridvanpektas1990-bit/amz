// eslint.config.mjs  — Flat Config für ESLint v9 / Next 15
import next from 'eslint-config-next';

export default [
  // Next.js Basis-Konfig
  ...next,
  // Unsere Overrides
  {
    files: ['**/*.{ts,tsx,js,jsx}'],
    rules: {
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/no-unused-vars': ['warn', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
    },
  },
];
