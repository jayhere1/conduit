# Contributing to Conduit

Thanks for your interest in Conduit. This document covers how to get set up,
the workflow for changes, and the legal sign-off every contribution needs.

By participating, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Ways to contribute

- **Report a bug** — open an issue with reproduction steps, the affected
  version (`conduit --version`) or commit, and what you expected.
- **Propose a change** — for anything non-trivial, open an issue first so we
  can agree on the approach before you write code.
- **Send a pull request** — see the workflow below.
- **Security issues** — do **not** open a public issue. Follow
  [SECURITY.md](SECURITY.md).

## Developer Certificate of Origin (sign-off required)

Conduit uses the [Developer Certificate of Origin](DCO) (DCO) rather than a
CLA. The DCO is a lightweight statement that you wrote the patch, or otherwise
have the right to contribute it under the project's license. It does **not**
transfer your copyright — you retain it.

Every commit must be signed off. Add the `-s` flag when you commit:

```bash
git commit -s -m "fix(scheduler): drain in-flight tasks on SIGTERM"
```

This appends a line to your commit message:

```
Signed-off-by: Your Name <your.email@example.com>
```

The name and email must match your real identity and your Git config
(`git config user.name` / `git config user.email`). Commits without a valid
sign-off cannot be merged. To sign off a series you already wrote:

```bash
git rebase --signoff main
```

## Licensing of contributions

Conduit is licensed under [Apache-2.0](LICENSE). Contributions are accepted
**inbound = outbound**: your patch is licensed to the project and its users
under Apache-2.0, the same terms the project ships under. You keep your
copyright; you are not assigning it.

> **Note for future maintainers:** the DCO does not, by itself, grant the
> project the right to re-license contributions under different terms. As long
> as Conduit stays Apache-2.0 this is exactly what we want. If the project ever
> pursues a commercial or source-available edition that would require
> re-licensing contributed code, adopt a signed Contributor License Agreement
> (e.g. CLA Assistant with an Apache-ICLA-based grant) **before** merging the
> first external contribution under those terms. Retrofitting consent from
> past contributors afterward is painful.

## Development setup

You need:

- **Rust** — install via [rustup](https://rustup.rs) (stable; the version CI
  uses is the source of truth). Conduit is a Cargo workspace on edition 2021.
- **Python 3.9+** — for running DAGs (task subprocesses) and the Python SDK
  tests.
- **Node.js** — only if you're touching the React UI (`conduit-ui`).
- **Docker** — only for the integration/E2E suites that spin up real databases.

Clone and build:

```bash
git clone https://github.com/jayhere1/conduit.git
cd conduit
cargo build            # debug build of the whole workspace
```

The [`Makefile`](Makefile) wraps the common tasks (`make help` lists them all):

| Command | What it does |
|---|---|
| `make build` | Debug build of all Rust crates |
| `make build-release` | Optimized release binary |
| `make test` | Full suite: Rust + Python SDK + UI |
| `make test-rust` | Rust workspace tests only |
| `make test-integration` | SQL provider tests (starts Docker databases) |
| `make lint` | Type-check and clippy |
| `make fmt` | Format all code |

See [TESTING.md](TESTING.md) for the full testing guide (integration gating,
env vars, the distributed chaos tests, and the Marquez round-trip).

## Pull request workflow

1. **Branch** off `main`. Keep PRs focused — one logical change per PR.
2. **Write tests.** New behavior needs coverage; bug fixes need a regression
   test that fails before your change.
3. **Run the checks locally** before pushing — CI runs the same ones and
   treats warnings as errors:
   ```bash
   cargo fmt --all
   cargo clippy --workspace --all-targets -- -D warnings
   cargo test --workspace
   ```
4. **Sign off** every commit (`git commit -s`, see above).
5. **Open the PR** against `main` and fill out the template. CI
   (`.github/workflows/ci.yml`) must be green: check, fmt, clippy, tests,
   Python SDK tests, and UI tests. Changes touching DAG/lineage schemas also
   run the schema-impact gate.
6. **Respond to review.** Push follow-up commits; we squash-or-merge on
   approval once CI is green.

## Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org):
`type(scope): summary` — e.g. `feat(api): enforce auth on mutating routes`,
`fix(lineage): resolve view-of-view columns`, `docs: …`, `test: …`,
`chore: …`. Keep the summary imperative and under ~72 characters.

## Project layout

Conduit is a Cargo workspace; each crate owns one concern
(`conduit-compiler`, `-scheduler`, `-executor`, `-planner`, `-lineage`,
`-api`, …). The `README.md` "Project Structure" section and
`docs/src/architecture.md` describe how they fit together. The Python SDK
lives in `sdk/python`; the PyO3 native bindings are `conduit-python`.

Questions that aren't a bug or feature request? Open a
[GitHub Discussion](https://github.com/jayhere1/conduit/discussions) or a
draft issue. Thanks for contributing.
