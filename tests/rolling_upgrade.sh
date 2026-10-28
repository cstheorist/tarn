#!/bin/bash
# Build the exact supported predecessor and this checkout, then test only
# owned processes/data. Retain build logs and all source/binary identities.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BASELINE=c3b3d14e2d46b9039af9971db7c313556b4a996c
WORK=$(mktemp -d /tmp/tarn-upgrade-build.XXXXXX)
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$ROOT/target}
mkdir -p "$WORK/old" "$WORK/artifacts"
git -C "$ROOT" cat-file -e "$BASELINE^{commit}" || {
  echo "Fetch the supported predecessor first: git fetch origin $BASELINE" >&2
  exit 1
}
git -C "$ROOT" archive "$BASELINE" | tar -x -C "$WORK/old"
echo "Upgrade build/evidence directory: $WORK"
cargo build --release --workspace --locked --manifest-path "$WORK/old/Cargo.toml" > "$WORK/old-build.log" 2>&1 || {
  tail -80 "$WORK/old-build.log"; exit 1;
}
mkdir -p "$WORK/old-native" "$WORK/new-native"
cp "$CARGO_TARGET_DIR/release/tarn-server" "$WORK/artifacts/old-server"
cp "$CARGO_TARGET_DIR/release/libtarn_storage.so" "$WORK/old-native/libtarn_storage.so"
cargo build --release --workspace --locked --manifest-path "$ROOT/Cargo.toml" > "$WORK/new-build.log" 2>&1 || {
  tail -80 "$WORK/new-build.log"; exit 1;
}
cp "$CARGO_TARGET_DIR/release/tarn-server" "$WORK/artifacts/new-server"
cp "$CARGO_TARGET_DIR/release/libtarn_storage.so" "$WORK/new-native/libtarn_storage.so"
EVIDENCE=${UPGRADE_EVIDENCE:-$WORK/results}
python3 "$ROOT/tests/rolling_upgrade.py" --old-root "$WORK/old" \
  --old-bin "$WORK/artifacts/old-server" --old-native "$WORK/old-native/libtarn_storage.so" \
  --new-bin "$WORK/artifacts/new-server" --new-native "$WORK/new-native/libtarn_storage.so" \
  --engine "${ENGINE:-memtx}" --evidence "$EVIDENCE"
echo "Upgrade results: $EVIDENCE"
