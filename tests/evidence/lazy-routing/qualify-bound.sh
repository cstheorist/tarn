#!/bin/bash
set -euo pipefail
mkdir -p /out/qualified-bound
for shape in uniform skewed; do
  for lanes in 1 4 16; do
    python3 tests/sql/lazy_routing_cost.py \
      --nodes 127.0.0.1:3311,127.0.0.1:3321,127.0.0.1:3331,127.0.0.1:3341 \
      --cluster-dir /tmp/issue185-qualification-r2 --output "/out/qualified-bound/$shape-$lanes.json" \
      --pairs 5 --seconds 10 --threads 32 --processes 4 --lanes "$lanes" --shapes "$shape" \
      --write-every 100 --prefix-every 0 --source 25046c4 \
      --binary /out/target/release/tarn-server --native /out/target/release/libtarn_storage.so \
      > "/out/qualified-bound/$shape-$lanes.log" 2>&1
  done
done
