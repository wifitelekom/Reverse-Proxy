#!/usr/bin/env bash
# demo.sh — hands-free walkthrough for the final project defense.
#
# Runs the headline pieces in sequence with a pause before each so you
# can narrate. Press Enter to advance, Ctrl-C to stop.
#
# Covers: test suite → stress/CV → heterogeneous selector → ARM cross-
# compile. It does NOT start the live cluster (that needs its own
# terminals) — run `./scripts/run_cluster.sh start` by hand for the finale.
#
# Usage:  ./scripts/demo.sh

set -u

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# --- pretty banners -------------------------------------------------------
bold() { printf "\033[1m%s\033[0m\n" "$1"; }
cyan() { printf "\033[36m%s\033[0m\n" "$1"; }
green() { printf "\033[32m%s\033[0m\n" "$1"; }
rule() { printf "\033[2m%s\033[0m\n" "------------------------------------------------------------"; }

step() {
  echo
  rule
  cyan "  $1"
  rule
  if [ -n "${2:-}" ]; then echo "  $2"; echo; fi
  printf "  \033[2m[Enter to run, Ctrl-C to stop]\033[0m "
  read -r _
}

# --- intro ----------------------------------------------------------------
clear 2>/dev/null || true
bold "P2P Gossip-Based HTTP Load Balancer — live demo"
echo "Project root: $PROJECT_ROOT"
echo "Go: $(go version 2>/dev/null || echo 'NOT FOUND — install Go 1.25+')"

# --- 1. test suite --------------------------------------------------------
step "1/4  Test suite (race detector)" \
     "255 tests across 6 packages, all run with -race. Watch every line end in 'ok'."
make test

# --- 2. stress / CV -------------------------------------------------------
step "2/4  Fair distribution under failure injection" \
     "1000 requests, kill a backend mid-run, 5% transient failures. Target CV < 15%."
go run ./experiments/stress

# --- 3. heterogeneous selector (the headline) -----------------------------
step "3/4  Selector comparison on a mixed 1ms/5ms/20ms cluster  [HEADLINE]" \
     "Watch P2C-EWMA's p99 — it should be ~3x lower than Round-Robin."
go run ./experiments/heterogeneous_bench

# --- 4. ARM cross-compile -------------------------------------------------
step "4/4  32-bit ARM portability (alignment-safe)" \
     "go vet's alignment analysis + a cross-compile for linux/arm. Both should exit clean."
echo "\$ GOOS=linux GOARCH=arm GOARM=5 go vet ./..."
if GOOS=linux GOARCH=arm GOARM=5 go vet ./... ; then green "  go vet: clean (no alignment diagnostics)"; fi
echo "\$ GOOS=linux GOARCH=arm GOARM=5 go build ./..."
if GOOS=linux GOARCH=arm GOARM=5 go build ./... ; then green "  cross-compile: OK"; fi

# --- outro ----------------------------------------------------------------
echo
rule
green "  Demo complete."
echo "  For the live cluster finale, in a fresh terminal run:"
echo "      ./scripts/run_cluster.sh build && ./scripts/run_cluster.sh start"
echo "      curl -s http://localhost:8001/members | python3 -m json.tool"
echo "      for i in \$(seq 1 8); do curl -s http://localhost:8080/ -w '\\\\n'; done"
echo "      go run ./cmd/simulator -cmd=kill -node=node-c   # watch failover"
echo "      ./scripts/run_cluster.sh stop"
rule
