#!/bin/bash
# Build the exact supported predecessor and this checkout, then test only
# owned processes/data. Retain build logs and all source/binary identities.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BASELINE=8b12c4525bd677499a91b973afbc86cbcbb0cafd
WORK=$(mktemp -d /tmp/tarn-upgrade-build.XXXXXX)
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
OLD_TARGET="$WORK/old-target"
NEW_TARGET="$WORK/new-target"
mkdir -p "$WORK/old" "$WORK/artifacts"
git -C "$ROOT" cat-file -e "$BASELINE^{commit}" || {
  echo "Fetch the supported predecessor first: git fetch origin $BASELINE" >&2
  exit 1
}
git -C "$ROOT" archive "$BASELINE" | tar -x -C "$WORK/old"
echo "Upgrade build/evidence directory: $WORK"
CARGO_TARGET_DIR="$OLD_TARGET" cargo build --release --workspace --locked --manifest-path "$WORK/old/Cargo.toml" > "$WORK/old-build.log" 2>&1 || {
  tail -80 "$WORK/old-build.log"; exit 1;
}
mkdir -p "$WORK/old-native" "$WORK/new-native"
cp "$OLD_TARGET/release/tarn-server" "$WORK/artifacts/old-server"
cp "$OLD_TARGET/release/libtarn_storage.so" "$WORK/old-native/libtarn_storage.so"
python3 "$ROOT/scripts/upgrade_build_receipt.py" --source "$WORK/old" --revision "$BASELINE" \
  --gateway "$WORK/artifacts/old-server" --native "$WORK/old-native/libtarn_storage.so" --output "$WORK/artifacts/old-build.json"
CARGO_TARGET_DIR="$NEW_TARGET" cargo build --release --workspace --locked --manifest-path "$ROOT/Cargo.toml" > "$WORK/new-build.log" 2>&1 || {
  tail -80 "$WORK/new-build.log"; exit 1;
}
cp "$NEW_TARGET/release/tarn-server" "$WORK/artifacts/new-server"
cp "$NEW_TARGET/release/libtarn_storage.so" "$WORK/new-native/libtarn_storage.so"
python3 "$ROOT/scripts/upgrade_build_receipt.py" --source "$ROOT" --revision "$(git -C "$ROOT" rev-parse HEAD)" \
  --gateway "$WORK/artifacts/new-server" --native "$WORK/new-native/libtarn_storage.so" --output "$WORK/artifacts/new-build.json"
EVIDENCE=${UPGRADE_EVIDENCE:-$WORK/results}
python3 "$ROOT/tests/rolling_upgrade.py" --old-root "$WORK/old" \
  --old-bin "$WORK/artifacts/old-server" --old-native "$WORK/old-native/libtarn_storage.so" \
  --new-bin "$WORK/artifacts/new-server" --new-native "$WORK/new-native/libtarn_storage.so" \
  --engine "${ENGINE:-memtx}" --evidence "$EVIDENCE"
echo "Upgrade results: $EVIDENCE"
