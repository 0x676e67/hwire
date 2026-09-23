# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## hwire [0.2.6](https://github.com/0x676e67/hwire/compare/v0.2.5...v0.2.6) - 2026-09-23



### Refactor

- *(http3)* Use named polling futures ([#82](https://github.com/0x676e67/hwire/issues/82)) - ([85530ad](https://github.com/0x676e67/hwire/commit/85530ada7a77a3b8d806c1c4bfe131e8d7d2e5a8))

### Miscellaneous Tasks

- Update hwire repository links ([#83](https://github.com/0x676e67/hwire/issues/83)) - ([7c0760d](https://github.com/0x676e67/hwire/commit/7c0760d467707c49cfd0329d18ebf40a0ffccde5))

## [0.2.5](https://github.com/0x676e67/wreq-proto/compare/wreq-proto-v0.2.4...wreq-proto-v0.2.5) - 2026-05-31

### Fixed

- *(http1)* fix busy loop when peer half-closes and open body ([#27](https://github.com/0x676e67/wreq-proto/pull/27))

### Other

- Update README.md
- Update README.md

## [0.2.4](https://github.com/0x676e67/wreq-proto/compare/wreq-proto-v0.2.3...wreq-proto-v0.2.4) - 2026-05-20

### Other

- *(lib)* replace unwraps with expects ([#22](https://github.com/0x676e67/wreq-proto/pull/22))
- *(lib)* use a panic_if_poisoned() helper for mutexes ([#21](https://github.com/0x676e67/wreq-proto/pull/21))

## [0.2.3](https://github.com/0x676e67/wreq-proto/compare/wreq-proto-v0.2.2...wreq-proto-v0.2.3) - 2026-05-10

### Other

- fmt deps
- fmt Cargo.toml
- Add wreq-rt ([#17](https://github.com/0x676e67/wreq-proto/pull/17))

## [0.2.2](https://github.com/0x676e67/wreq-proto/compare/v0.2.1...v0.2.2) - 2026-05-08

### Fixed

- *(http2)* do not reserve capacity before body data is available ([#15](https://github.com/0x676e67/wreq-proto/pull/15))

### Other

- "build(deps): reduce dependency on futures-channel" ([#16](https://github.com/0x676e67/wreq-proto/pull/16))
- Update README.md

## [0.2.1](https://github.com/0x676e67/wreq-proto/compare/v0.2.0...v0.2.1) - 2026-04-29

### Other

- *(body)* fix docs build ([#12](https://github.com/0x676e67/wreq-proto/pull/12))
- fmt

## [0.2.0](https://github.com/0x676e67/wreq-proto/compare/v0.1.0...v0.2.0) - 2026-04-29

### Other

- Fix capitalization in package description
- *(ext)* rename method to `call_visit` and clarify its purpose ([#10](https://github.com/0x676e67/wreq-proto/pull/10))

## [0.1.0](https://github.com/0x676e67/wreq-proto/releases/tag/v0.1.0) - 2026-04-29

### Added

- *(ext)* add `ext::on_preserve_header()` callback extension ([#7](https://github.com/0x676e67/wreq-proto/pull/7))
- *(ext)* add `ext::on_informational()` callback extension ([#6](https://github.com/0x676e67/wreq-proto/pull/6))
- *(rt)* runtime-agnostic ([#5](https://github.com/0x676e67/wreq-proto/pull/5))

### Fixed

- fix bench
- fix bench

### Other

- release-plz
- remove unused mod
- Update README.md
- update package description in Cargo.toml
- remove parking_lot
- remove unused mod
- update tests ([#4](https://github.com/0x676e67/wreq-proto/pull/4))
- Update Cargo.toml
- Update README.md
- Bump actions/cache from 3 to 5 ([#3](https://github.com/0x676e67/wreq-proto/pull/3))
- Add default feature and tracing logging
- Configure Dependabot for GitHub Actions and Cargo
- Refactor benchmark workflow by cleaning up steps
- Fix cargo bench command by removing features flag
- Update bench.yml
- Update copyright information in LICENSE file
- Delete .github/workflows/rust.yml
- Merge pull request #2 from 0x676e67/bench
- Add GitHub Actions workflow for Rust project
- update
- init commit
- Initial commit
