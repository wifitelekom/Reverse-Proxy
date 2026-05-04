# Makefile — local developer ergonomics for the p2p load balancer.
#
# Targets here mirror what CI runs (.github/workflows/test.yml). If you can
# `make ci` clean locally, the GitHub Actions run will (modulo platform
# differences) be clean too. Keep that contract.
#
# Conventions:
#   - `make` (no args) prints help. Discoverability beats memorisation.
#   - Every test invocation passes -race; we have enough concurrent state
#     that the cost of -race is cheaper than the cost of a missed race.
#   - -count=1 disables the test cache. We hit it explicitly here because
#     a flaky test re-running from cache hides the flake.
#   - The -tags toml variant exercises the TOML config loader (compiled in
#     only with that tag — see pkg/config/toml_enabled.go).

GO              ?= go
GOFLAGS         ?=
PKGS            ?= ./...
TEST_TIMEOUT    ?= 5m
COVERAGE_FILE   ?= coverage.out
COVERAGE_HTML   ?= coverage.html
BIN_DIR         ?= bin
LB_BIN          ?= $(BIN_DIR)/loadbalancer
NODE_BIN        ?= $(BIN_DIR)/node
SIM_BIN         ?= $(BIN_DIR)/simulator

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help — list targets with their first comment line.
# Convention: `target: ## description` lines auto-populate this.
# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help and exit
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
.PHONY: test
test: ## Run all tests with -race (default build tags)
	$(GO) test -race -count=1 -timeout $(TEST_TIMEOUT) $(GOFLAGS) $(PKGS)

.PHONY: test-toml
test-toml: ## Run all tests with -race and -tags toml
	$(GO) test -race -count=1 -timeout $(TEST_TIMEOUT) -tags toml $(GOFLAGS) $(PKGS)

.PHONY: test-all
test-all: test test-toml ## Run both default and toml test variants

.PHONY: coverage
coverage: ## Produce coverage profile + HTML report (race-enabled)
	$(GO) test -race -count=1 -timeout $(TEST_TIMEOUT) -coverprofile=$(COVERAGE_FILE) $(PKGS)
	$(GO) tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "Coverage report: $(COVERAGE_HTML)"

# ---------------------------------------------------------------------------
# Static checks (mirrors what CI enforces)
# ---------------------------------------------------------------------------
.PHONY: vet
vet: ## Run `go vet` for both default and toml build tags
	$(GO) vet $(PKGS)
	$(GO) vet -tags toml $(PKGS)

.PHONY: fmt
fmt: ## Format all Go files in place with gofmt
	gofmt -w .

.PHONY: fmt-check
fmt-check: ## Fail if any Go file is not gofmt-clean
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt found unformatted files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

.PHONY: tidy
tidy: ## Run `go mod tidy`
	$(GO) mod tidy

.PHONY: tidy-check
tidy-check: ## Fail if go.mod / go.sum drift from `go mod tidy`
	@$(GO) mod tidy
	@git diff --exit-code -- go.mod go.sum || { \
		echo "go.mod / go.sum out of date; run 'make tidy' and commit"; \
		exit 1; \
	}

# ---------------------------------------------------------------------------
# Aggregate gates
# ---------------------------------------------------------------------------
.PHONY: ci
ci: tidy-check fmt-check vet test-all ## Run everything CI runs (use before pushing)

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
.PHONY: build
build: $(LB_BIN) $(NODE_BIN) $(SIM_BIN) ## Build all binaries into $(BIN_DIR)

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

$(LB_BIN): | $(BIN_DIR)
	$(GO) build -o $(LB_BIN) ./cmd/loadbalancer

$(NODE_BIN): | $(BIN_DIR)
	$(GO) build -o $(NODE_BIN) ./cmd/node

$(SIM_BIN): | $(BIN_DIR)
	$(GO) build -o $(SIM_BIN) ./cmd/simulator

.PHONY: build-toml
build-toml: | $(BIN_DIR) ## Build the load balancer with TOML config support
	$(GO) build -tags toml -o $(LB_BIN) ./cmd/loadbalancer

.PHONY: clean
clean: ## Remove build artefacts and coverage files
	rm -rf $(BIN_DIR) $(COVERAGE_FILE) $(COVERAGE_HTML)
