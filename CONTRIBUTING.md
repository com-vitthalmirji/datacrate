# Contributing

This is a solo-maintained project, but it follows the same discipline as a
shared repo so history stays readable and CI stays trustworthy.

## Before you start

1. `just verify` must pass on a clean checkout before you branch.
2. Run `just install-hooks` once per clone to enable the pre-commit check
   (`just verify` before every commit - see `.githooks/pre-commit`).

## Branching

[Trunk-based / GitHub Flow](https://docs.aws.amazon.com/prescriptive-guidance/latest/choosing-git-branch-approach/git-branching-strategies.html):
short-lived branches off `main`, merged back once `just verify` passes. No
long-lived `develop`/`release` branches - Git Flow's ceremony isn't earned
by a project this size.

- Branch names: `<type>/<short-description>`, e.g. `feat/csv-select-cli`,
  `fix/header-clone`, `docs/readme-quickstart`.
- Keep branches short-lived (days, not weeks). Rebase on `main` before merging
  rather than letting branches drift.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):

```
<type>(<optional scope>): <description>

<optional body>

<optional footer>
```

Common types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`, `perf`.
Breaking changes get a `!` after the type/scope (`feat(csv-cli)!: ...`) plus
a `BREAKING CHANGE:` footer explaining the impact.

- Subject line: imperative mood, lowercase after the type, no trailing period,
  ideally under 50 characters and never over 72.
- Explain *why* in the body when the change isn't self-evident from the diff.

## Pull requests

Even reviewing your own PRs is useful - it's a second look before it lands on
`main`.

- One logical change per PR. Don't bundle unrelated fixes.
- PR description: what changed and why, not a restatement of the diff.
- `just verify` (fmt, clippy with warnings denied, tests, release build, `git
  diff --check`) must pass locally and in CI before merging.

## Code style

- Formatting and lint configuration live in `rustfmt.toml` and the
  `[workspace.lints.clippy]` table in the root `Cargo.toml` - don't fight
  them with inline `#[allow(...)]` unless the reason is commented.
- No `unwrap()` on any reachable path (enforced by `clippy::unwrap_used` +
  `-D warnings`). `.expect()` is reserved for invariants that indicate bugs,
  and for test assertions.
