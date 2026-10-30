set -e
export TARN_BIN=/tmp/tarn176-artifacts-v7/tarn-server TARN_NATIVE=/tmp/tarn176-artifacts-v7/libtarn_storage.so RUST_MIN_STACK=16777216 TARN_CHECK_YIELDS=1 TARN_CHECK_LOCKS=1
cd /tmp/tarn176-integrated-source
TARN_SOURCE_REVISION=b90abdf8f5bcc8cd0f14298acca248b787266081 python3 tests/bounded_moves.py --work /tmp/tarn176-bootstrap-prior --engine mixed --mib 1 --fault source --traffic
cd /tmp/tarn176-c654-source
TARN_SOURCE_REVISION=c654eae90df29db2c5f8e8ac93fa8016c4117317 python3 tests/bounded_moves.py --work /tmp/tarn176-c654-v2-native-source --engine mixed --mib 1 --fault source --traffic
