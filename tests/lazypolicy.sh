#!/bin/bash
# Private RF3/fsync policy and leader-crash controls, both native engines.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=$(mktemp -d /tmp/tarn-lazy-policy.XXXX)
BASE=${BASE:-6200}; PORT=${PORT:-6300}
export TARN_TEST_HOOKS=1 TARN_CHECK_YIELDS=1 TARN_CHECK_LOCKS=1
export TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0
cleanup() {
    local result=$?
    for engine in memtx vinyl; do
        KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR/$engine" >/dev/null 2>&1 || true
    done
    echo "lazy policy evidence: $DIR"
    if [ "$result" != 0 ]; then
        for f in "$DIR"/*/*.log; do [ ! -f "$f" ] || { echo "--- $f"; tail -n 50 "$f"; }; done
    fi
}
trap cleanup EXIT
find "$ROOT/lua" "$ROOT/crates/tarn-sql" "$ROOT/crates/tarn-storage" "$ROOT/crates/tarn-verified" \
    -type f -print0 | sort -z | xargs -0 sha256sum > "$DIR/source.sha256"
sha256sum "$ROOT/tests/sql/lazytest.py" "$ROOT/tests/sql/lazypolicyfaults.py" "$ROOT/tests/lazypolicy.sh" \
    "$ROOT/Cargo.lock" "$ROOT/rust-toolchain.toml" >> "$DIR/source.sha256"
sha256sum "${TARN_BIN:-$ROOT/target/release/tarn-server}" > "$DIR/gateway.sha256"
native=${TARN_NATIVE:-$ROOT/target/release/libtarn_storage.so}
if [ "$native" != 0 ] && [ -f "$native" ]; then sha256sum "$native" > "$DIR/native.sha256"; fi
"${TARANTOOL:-tarantool}" --version > "$DIR/tarantool-version.txt"
for engine in memtx vinyl; do
    "$ROOT/scripts/cluster.sh" start -n 2 -r 3 -w fsync -m 128 -d "$DIR/$engine" \
        -b "$BASE" -p "$PORT" -e "$engine"
    nodes=()
    for shard in 1 2; do
        found=''
        for replica in 1 2 3; do
            node="127.0.0.1:$((BASE+10*shard+replica))"
            if "${TARANTOOL:-tarantool}" "$ROOT/scripts/probe.lua" "tarn:tarn@$node" >/dev/null 2>&1; then found=$node; break; fi
        done
        [ -n "$found" ] || { echo 'FAIL no RF3 leader'; exit 1; }
        nodes+=("$found")
    done
    python3 "$ROOT/tests/sql/lazytest.py" --tarn "127.0.0.1:$PORT" --kv "${nodes[0]},${nodes[1]}" \
        --engine "$engine" --only consistency | tee "$DIR/$engine/policy.log"
    python3 "$ROOT/tests/sql/lazypolicyfaults.py" --tarn "127.0.0.1:$PORT" --cluster-dir "$DIR/$engine" \
        --engine "$engine" | tee "$DIR/$engine/fault.log"
    KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR/$engine"
done
