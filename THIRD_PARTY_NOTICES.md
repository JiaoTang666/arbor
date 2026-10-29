# Third-party notices

## BlockEmulator

The Go engine is derived from [BlockEmulator](https://github.com/HuangLab-SYSU/block-emulator), Copyright (c) 2023 HuangLab-SYSU, distributed under MIT. The complete original notice remains at `native/engine/LICENSE`. The engine includes subsequent experimental protocol and telemetry extensions; repository provenance records identify the extracted source files. The upstream project does not endorse these modifications.

[Native engine guide](docs/native-engine.md) identifies the supported method paths and inherited extensions, including integration tests that require external fixtures.

## Dependencies

`native/engine/go.mod` and `go.sum` identify the Go dependency versions; `requirements.lock` identifies Python packages. They retain their respective upstream licenses. Dependencies are downloaded by the build tools, not copied into this repository. Run `go list -m all` inside `native/engine` to inspect the Go dependency graph.

The installer obtains Go from `go.dev` when necessary and pip from PyPI. Downloaded compilers, virtual environments and built binaries are local artifacts excluded from Git.

## Data and models

Transaction data is obtained from the [XBlock Block Transaction collection](https://xblock.pro/xblock-eth.html). This repository provides official source links, processing code and reference metadata. The included fitted model parameters were produced for Arbor; model provenance is documented in `docs/data-models.md`.

The names Monoxide and CLPA identify the corresponding execution/partition baselines implemented on this engine. This repository is an experimental reproduction, not an official distribution of those projects.

Original publications, upstream implementation links and baseline scope are listed in [docs/references.md](docs/references.md).
