set -e
cd /tmp/tarn176-c654-source
export TARN_BIN=/tmp/tarn176-artifacts-v7/tarn-server TARN_SOURCE_REVISION=c654eae90df29db2c5f8e8ac93fa8016c4117317
export RUST_MIN_STACK=16777216 TARN_CHECK_YIELDS=1 TARN_CHECK_LOCKS=1
export TARN_NATIVE=/tmp/tarn176-artifacts-v7/libtarn_storage.so
python3 tests/bounded_moves.py --work /tmp/tarn176-c654-v2-native-receiver --engine mixed --mib 1 --fault receiver --traffic
export TARN_NATIVE=0
for fault in source receiver; do
 python3 tests/bounded_moves.py --work /tmp/tarn176-c654-v2-lua-$fault --engine mixed --mib 1 --fault "$fault" --traffic
done
