# 0009. Gate the dependency tree with cargo-audit and cargo-deny, with recorded exceptions

## Status
Accepted

## Context
Adding Ballista, Comet's benchmark tooling, and object-store clients (see
[0008](0008-ballista-and-comet-benchmark-engines.md)) expanded the dependency tree considerably.
`cargo-audit` alone catches known vulnerabilities but says nothing about license compatibility,
duplicate dependency versions, or where a dependency's source is allowed to come from. Two
advisories fired that needed a judgment call rather than being ignored or blocked outright:

- Two RUSTSEC advisories: a quick-xml denial-of-service pattern pulled transitively through the
  object-store crate's S3 XML response parsing. The XML being parsed comes from trusted
  infrastructure, not untrusted user input, so the DoS vector doesn't apply here.
- An unmaintained-crate flag on `bincode`, pulled transitively through a Polars dependency and
  never called directly by this pipeline's code.

## Decision
Added `cargo-deny` as a complementary gate to `cargo-audit`, covering license policy, a
banned/duplicate-dependency-version check, and a source allow-list restricting where dependencies
are permitted to come from. Both tools carry explicit, individually justified ignore entries for
the three advisories above, recorded with the reasoning rather than suppressed silently. The same
advisory IDs were also added to the CI audit workflow's ignore list, matching the local `cargo-deny`
configuration.

## Consequences
The dependency tree is checked on two axes (known vulnerabilities, and license/source/duplication
policy) instead of one, and every suppressed advisory has a written justification attached to it
rather than existing only as an unexplained ignore line — anyone auditing the policy later can see
why each one was judged safe. The tradeoff is that these ignore entries need periodic revisiting:
an advisory judged safe under "trusted infrastructure input only" stops being safe if the pipeline
ever starts parsing object-store responses from an untrusted source.
