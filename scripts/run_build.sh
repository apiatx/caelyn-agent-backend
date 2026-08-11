#!/usr/bin/env bash
# CaelynAI canonical build script.
#
# Used by:
#   - Replit deployment build  (.replit [deployment].build)
#   - workspace_guard.py prepublish and prepush (build validation step)
#
# Exits NONZERO on any compile failure.
# There is deliberately NO trailing `true` — compile failures must NOT be masked.
#
# To run locally:
#   bash scripts/run_build.sh

set -euo pipefail

BACKEND="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/backend"
PYTHONLIBS="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.pythonlibs"

echo "[BUILD] Compiling backend source..."
python3.11 -m compileall -q \
  "${BACKEND}/agent" \
  "${BACKEND}/core" \
  "${BACKEND}/data" \
  "${BACKEND}/routes" \
  "${BACKEND}/services" \
  "${BACKEND}/scripts" \
  "${BACKEND}"/*.py

echo "[BUILD] Compiling .pythonlibs..."
if [[ -d "$PYTHONLIBS" ]]; then
  python3.11 -m compileall -q "$PYTHONLIBS"
else
  echo "[BUILD] .pythonlibs not found — skipping (not installed yet)."
fi

echo "[BUILD] Done — no compile errors."
