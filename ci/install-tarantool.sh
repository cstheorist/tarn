#!/bin/bash
# Build Tarantool from source and install it under PREFIX, unless PREFIX
# already holds that version (CI caches PREFIX).
#
#   ci/install-tarantool.sh [VERSION] [PREFIX]     defaults: 3.9.0, ~/.local/tarantool
set -euo pipefail
VERSION=${1:-3.9.0}
PREFIX=${2:-$HOME/.local/tarantool}
if [ -x "$PREFIX/bin/tarantool" ] && "$PREFIX/bin/tarantool" --version | head -1 | grep -q "Tarantool $VERSION-"; then
  echo "using cached $("$PREFIX/bin/tarantool" --version | head -1)"
  exit 0
fi
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential cmake autoconf automake libtool git \
  zlib1g-dev libreadline-dev libncurses-dev libssl-dev libicu-dev
SRC=$(mktemp -d)
git clone --quiet --depth 1 --branch "$VERSION" --recurse-submodules \
  https://github.com/tarantool/tarantool.git "$SRC/tarantool"
cmake -S "$SRC/tarantool" -B "$SRC/build" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" -DENABLE_BACKTRACE=TRUE
cmake --build "$SRC/build" --parallel "$(nproc)"
cmake --install "$SRC/build"
"$PREFIX/bin/tarantool" --version | head -2
