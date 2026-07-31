# Contributing to Thunders BigData System

Thank you for your interest in contributing! This document explains the
workflow we use to keep the project healthy.

## Getting started

1. Fork the repository and clone your fork.
2. Run `make setup` to install dependencies and pre-commit hooks.
3. Create a feature branch: `git checkout -b feat/your-feature`.
4. Make your changes, keeping commits focused.
5. Run `make lint test` locally before pushing.

## Commit conventions

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` a new feature
- `fix:` a bug fix
- `docs:` documentation only
- `refactor:` code change that neither fixes a bug nor adds a feature
- `perf:` performance improvement
- `test:` adding tests
- `chore:` tooling or maintenance

## Pull request process

1. Open a pull request against `develop`.
2. Fill in the PR template.
3. Make sure CI is green.
4. Request review from a CODEOWNER.
5. Squash-merge once approved.

## Code style

- Python: `black` + `ruff` + `mypy`.
- TypeScript/JavaScript: `eslint` + `prettier`.
- Keep functions small and well-named.
- Prefer composition over inheritance.

## Reporting issues

Use the issue templates under `.github/ISSUE_TEMPLATE/`. Provide enough
context for a maintainer to reproduce the problem.
