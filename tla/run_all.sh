#!/bin/bash
# Model-check every configuration; summarize outcome, states and time.
cd "$(dirname "$0")"; mkdir -p out
# tla2tools.jar: $TLA2TOOLS, or tla/tla2tools.jar (see docs/verification.md)
JAR=${TLA2TOOLS:-tla2tools.jar}
[ -f "$JAR" ] || { echo "tla2tools.jar not found: set TLA2TOOLS or download it into tla/" >&2; exit 2; }
for cfg in "$@"; do
  # The grouped-read modules check local snapshot and DDL-claim premises;
  # every other configuration uses the commit-protocol MC module.
  case "$cfg" in
    GroupedRead|Bug_GroupedReadSeparateSnapshots) module=GroupedRead.tla ;;
    GroupedDDL|Bug_GroupedDDLIgnoreGeneration|Bug_GroupedDDLIgnoreStale) module=GroupedDDL.tla ;;
    *) module=MC.tla ;;
  esac
  start=$(date +%s)
  java -XX:+UseParallelGC -Xmx${TLC_HEAP:-3g} -cp "$JAR" tlc2.TLC -workers ${WORKERS:-1} -cleanup \
       -metadir ${TLC_META:-/tmp/tlcmeta}/$cfg -config $cfg.cfg "$module" > out/$cfg.out 2>&1
  secs=$(( $(date +%s) - start ))
  res=$(grep -oE "Invariant [A-Za-z]+ is violated" out/$cfg.out | head -1)
  [ -z "$res" ] && res=$(grep -q "No error has been found" out/$cfg.out && echo "no errors" || echo "ERROR: $(grep -m1 -iE 'error|exception' out/$cfg.out)")
  st=$(grep -oE "[0-9]+ distinct states found" out/$cfg.out | tail -1)
  depth=$(grep -oE "depth of the complete state graph search is [0-9]+" out/$cfg.out | grep -oE "[0-9]+$")
  printf "%-28s %-45s %-28s depth=%-4s %5ss\n" "$cfg" "$res" "$st" "${depth:--}" "$secs"
done
