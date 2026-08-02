# QALITA Public Packs

<p align="center">
  <img width="250px" height="auto" src="https://app.platform.qalita.io/logo.svg" style="max-width:250px;"/>
</p>

[![CI Pipeline](https://github.com/qalita/packs/actions/workflows/publish-packs.yml/badge.svg)](https://github.com/qalita/packs/actions/workflows/publish-packs.yml)

Welcome to QALITA's public packs repository. All packs in this repository are open source and free to use. You can use these packs to create your own QALITA projects or to contribute to the QALITA community.

## What is a pack?

A pack is a collection of assets that can be used to perform a specific type of analysis.

Checkout [Documentation](https://doc.qalita.io/docs/platform/user-guides/data-engineering/packs/) if  you want to know more about packs.

## How to use a pack?

You can use a pack by importing it into your QALITA project. Once imported, you can use the assets in the pack to perform analysis on your dataset.

## How to contribute?

You can contribute to this repository by forking it and submitting a pull request. You can also create an issue to report a bug or to request a new feature.

### Release order: `qalita-core` first

Every pack declares a `qalita-core` floor in its `pyproject.toml`, and
`scripts/run.sh` runs `uv lock` on the worker for every job. When a change here
needs a new core API, the matching `qalita-core` version must be **tagged on
`qalita/core`'s `main` and published to PyPI before this repository's `main`
receives the work** — merging to `main` publishes every pack immediately, and a
pack pinning an unpublished core fails to resolve on every worker.

The `resolve` job in `.github/workflows/publish.yml` checks this on each pull
request and blocks publishing if any pack cannot be resolved.

## License

All packs in this repository are licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0). You can use these packs for free, but you must include the original copyright notice.
