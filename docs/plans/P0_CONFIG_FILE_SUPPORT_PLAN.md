# P0 Plan: Configuration File Support (YAML/TOML)

## 1. Objective

Introduce file-based configuration for node, load balancer, and simulator workflows to complement CLI flags and make multi-environment operation repeatable.

Primary goals:

- predictable startup in dev/stage/prod
- easier cluster reproducibility
- reduced operational error from long flag strings

## 2. Why This Is P0

- CLI-only configuration does not scale well for multiple environments.
- Reproducing exact setups for demos/tests is error-prone.
- Team workflows and CI/CD benefit from declarative config files.

## 3. Scope

In scope:

- Add config file loading for `cmd/node` and `cmd/loadbalancer`.
- Support one file format in v1 with clear extension path:
  - prefer TOML first (stdlib-aligned project style), YAML optional next step.
- Define precedence:
  - defaults < config file < CLI flags
- Provide sample config files for local cluster.

Out of scope:

- Dynamic config reload/hot-reload.
- Remote config stores.
- Full schema registry/version migration system.

## 4. Target Behavior

CLI additions:

- `-config=<path>` flag for `node` and `loadbalancer`.

Loading behavior:

1. Initialize defaults (`types.DefaultConfig()` and LB defaults).
2. If `-config` is provided, read file and merge values.
3. Apply CLI flags as final override.
4. Run existing validation and fail fast with clear errors.

Environment support:

- Keep existing scripts functional.
- Add example files for:
  - local dev cluster
  - optional stage-like profile

## 5. Proposed Config Structure

Node config sections:

- `node.id`
- `node.gossip_addr`
- `node.service_addr`
- `node.seed_nodes`
- `protocol.*` (timings, fanout, suspect thresholds)
- `debug.enable`

Load balancer config sections:

- `lb.listen_addr`
- `lb.strategy`
- `lb.gossip_addr`
- `lb.seed_nodes`
- `lb.retry.*`
- `lb.health_check.*` (for future/paired P0 feature)
- `protocol.*`

## 6. Implementation Plan

1. Add config model structs dedicated to file parsing (decoupled from runtime structs).
2. Implement parser package under `pkg/config`:
   - file read
   - decode
   - merge into runtime `types` configs
3. Add `-config` handling in `cmd/node/main.go`.
4. Add `-config` handling in `cmd/loadbalancer/main.go`.
5. Keep backward compatibility: existing flag-only runs must remain valid.
6. Add sample files under `configs/` and update developer docs.

## 7. Testing Plan

Unit tests:

- Valid config file parse and mapping.
- Missing required fields fail with descriptive errors.
- Invalid duration/number formats fail fast.
- Precedence tests:
  - defaults only
  - defaults + file
  - defaults + file + flags (flags win)

Command-level tests:

- Node and LB start correctly with only `-config`.
- Node and LB start correctly with mixed `-config` + overrides.

Regression:

- Existing script-driven startup without `-config` remains unchanged.

## 8. Acceptance Criteria

- Both binaries support `-config` and boot successfully from file-based settings.
- Config precedence is deterministic and documented.
- Sample configs can reproduce local cluster behavior.
- Existing tests pass; new config tests pass.

## 9. Risks and Mitigations

Risk: parser/dependency complexity conflicts with low-dependency philosophy  
Mitigation: start with minimal parser strategy and isolate parser package boundary.

Risk: ambiguous precedence between flags and file values  
Mitigation: enforce and document single precedence rule (flags always override).

Risk: drift between config schema and runtime structs  
Mitigation: explicit mapping functions + validation tests.
