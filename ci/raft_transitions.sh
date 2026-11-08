#!/bin/bash
# Repeat each storage node's last role changes as one GitHub annotation per
# node: elections, a leader's resignation, read-only switches and strict
# fencing pauses. A failed step whose cluster logs are only in the artifact
# then shows why a shard had no leader on the pull request itself.
#   ci/raft_transitions.sh TITLE LOG...
title=$1; shift
for log in "$@"; do
  [ -f "$log" ] || continue
  node=$(basename "$log" .log)
  changes=$(grep -E 'RAFT: (bump term|vote for|enter (candidate|leader) state|leader is|resigning|begin new election|fencing (paused|resumed))|box switched to' "$log" |
    tail -n 6 | sed -E 's/^[0-9-]+ ([0-9:.]+) \[[0-9]+\] [^ ]+ [^ ]+ [A-Z]> /\1 /' | cut -c1-150 | paste -sd'|' - | sed 's/|/ | /g')
  [ -n "$changes" ] || continue
  printf '::error title=%s %s::%s\n' "$title" "$node" "$(printf '%s' "$changes" | sed 's/%/%25/g')"
done
exit 0
