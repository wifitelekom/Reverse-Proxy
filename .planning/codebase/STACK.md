# STACK

## Overview
- Language/runtime: Go 1.21 (`go.mod`).
- Dependency model: standard-library only; no third-party modules are declared.
- Build/run model: three binaries (`node`, `loadbalancer`, `simulator`) built from `cmd/*`.
- OS tooling: shell and batch scripts for Linux/macOS/Windows cluster lifecycle.

## Evidence (specific files)
- `go.mod`
- `cmd/node/main.go`
- `cmd/loadbalancer/main.go`
- `cmd/simulator/main.go`
- `scripts/run_cluster.sh`
- `scripts/run_cluster.bat`
- `README.md`

## Current behavior
- `go build` produces standalone binaries under `bin/`.
- Runtime uses UDP for gossip membership and HTTP for service/proxy traffic.
- Logging uses `log/slog` JSON/text handlers from the standard library.
- Tests use Go `testing`, `httptest`, and real UDP sockets for protocol integration checks.

## Risks/Gaps
- No CI/workflow files in repo to enforce test/race/static checks automatically.
- Fixed default ports in scripts and simulator reduce portability for parallel environments.
- Operational defaults enable debug endpoints in helper scripts (`ENABLE_DEBUG=1`).

## Recommendations
- Add a CI pipeline with `go test ./...` and `go test -race ./...`.
- Parameterize script ports/node count through flags/env vars.
- Separate dev/test startup profile from safer production profile defaults.

