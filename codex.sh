#!/usr/bin/env bash
export CODEX_HOME="$PWD/.codex"
exec npx -y @openai/codex \
  --ask-for-approval never \
  --sandbox danger-full-access \
  "$@"
