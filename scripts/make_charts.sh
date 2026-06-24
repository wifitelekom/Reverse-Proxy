#!/usr/bin/env bash
# make_charts.sh — run the experiments and render thesis-grade chart PNGs.
#
# One command, end to end:
#   1. creates a Python venv with matplotlib (first run only)
#   2. runs the four data-producing experiments, saving JSON
#   3. renders PNG charts into ./charts/ (or $1)
#
# Why a venv: Homebrew Python blocks global `pip install` (PEP 668), so
# we keep matplotlib in an isolated environment under /tmp.
#
# Usage:
#   ./scripts/make_charts.sh [out_dir]        # default out_dir = ./charts
#
# Re-run this right before the defense so the numbers on the charts match
# whatever you quote out loud (the stress CV and exact ms vary a little
# run to run; the relationships do not).

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

OUT_DIR="${1:-$PROJECT_ROOT/charts}"
DATA_DIR="$(mktemp -d)"
VENV="${CHART_VENV:-/tmp/chartenv}"

echo "==> Python venv + matplotlib"
if [ ! -x "$VENV/bin/python" ]; then
    python3 -m venv "$VENV"
fi
"$VENV/bin/python" -c "import matplotlib" 2>/dev/null || \
    "$VENV/bin/pip" install --quiet matplotlib

echo "==> Running experiments (this takes ~2-3 min; mTLS 100k is the slow one)"
echo "    stress..."        && go run ./experiments/stress              > "$DATA_DIR/stress.json" 2>/dev/null
echo "    heterogeneous..." && go run ./experiments/heterogeneous_bench > "$DATA_DIR/hetero.json" 2>/dev/null
echo "    mtls (100k)..."   && go run ./experiments/mtls_bench -samples 100000 > "$DATA_DIR/mtls.json" 2>/dev/null
echo "    soak (90s)..."    && go run ./experiments/soak -duration 90s -sample-ms 3000 > "$DATA_DIR/soak.json" 2>/dev/null

echo "==> Rendering charts -> $OUT_DIR"
"$VENV/bin/python" experiments/charts/make_charts.py "$DATA_DIR" "$OUT_DIR"

rm -rf "$DATA_DIR"
echo "==> Done. PNGs in $OUT_DIR/"
ls -1 "$OUT_DIR"/*.png 2>/dev/null || true
