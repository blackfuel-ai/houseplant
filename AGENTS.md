# AGENTS.md

Blackfuel's fork of [juneHQ/houseplant](https://github.com/juneHQ/houseplant), the ClickHouse migration CLI. Its one consumer is `blackfuel-ai/ai-platform`, whose `clickhouse/pyproject.toml` pins a fork tag through `[tool.uv.sources]` (`houseplant = { git = "https://github.com/blackfuel-ai/houseplant.git", rev = "<tag>" }`) and whose `clickhouse/uv.lock` records the commit that tag points at.

## Branches

- `master` is the fork's only long-lived branch and the only branch released from. Every change reaches it through a pull request, merged with a merge commit, never squashed.
- One pull request carries one fix or one feature.
- `upstream` is `nlenepveu/houseplant`; upstream changes enter through a pull request onto `master` like any other change.

## Tagging

- **Only a commit on `master` is tagged.** A feature branch, a release branch or a merge made outside a pull request is never tagged, so every released commit is reachable from `master` and the next release contains it.
- **Form:** `v<upstream version>+bfai.<n>`, e.g. `v0.2.5+bfai.7`. `<upstream version>` is the `version` in `pyproject.toml`; `<n>` counts the fork's releases on that upstream version, one higher than the previous tag, and starts at 1 when the upstream version changes.
- **Annotated, with the tag name as its message**, on the `master` merge commit being released:
  - `git tag -a 'v0.2.5+bfai.7' -m 'v0.2.5+bfai.7' <master commit>`
  - `git push origin 'refs/tags/v0.2.5+bfai.7'`
- **A tag a consumer has locked is never moved or deleted.** `uv.lock` records the tagged commit, so a moved tag breaks `uv lock --check` in the consumer. A fix ships as the next `<n>`.
- **No package index.** Consumers install from the git tag. PyPI rejects the `+bfai.<n>` local version label, so nothing is published there.

## Releasing to ai-platform

1. Merge the pull requests for the release into `master`, and run the tests on the resulting `master` commit.
2. Tag that commit as above.
3. In ai-platform, set `rev` in `clickhouse/pyproject.toml` to the new tag, run `uv lock` in `clickhouse/`, and open the pull request there, naming the fork pull requests it ships.

## Tests

The suite needs a ClickHouse server on `localhost:9000`, e.g. `docker run -d -p 127.0.0.1:9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:26.8-alpine`. Then `uv run pytest tests` and `uv run ruff check`, the commands `.github/workflows/test.yml` runs.
