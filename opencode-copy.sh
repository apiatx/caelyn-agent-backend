#!/usr/bin/env bash
set -e

cd /home/runner/workspace || exit 1

export OPENCODE_DISABLE_MOUSE=true

PERSIST_ROOT="/home/runner/workspace/.opencode-persistent"

mkdir -p "$PERSIST_ROOT/share"
mkdir -p "$PERSIST_ROOT/config"
mkdir -p "$PERSIST_ROOT/state"

mkdir -p /home/runner/.local/share
mkdir -p /home/runner/.config
mkdir -p /home/runner/.local/state

rm -rf /home/runner/.local/share/opencode
rm -rf /home/runner/.config/opencode
rm -rf /home/runner/.local/state/opencode

ln -s "$PERSIST_ROOT/share" /home/runner/.local/share/opencode
ln -s "$PERSIST_ROOT/config" /home/runner/.config/opencode
ln -s "$PERSIST_ROOT/state" /home/runner/.local/state/opencode

exec opencode "$@"
