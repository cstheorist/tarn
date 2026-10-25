#!/bin/bash
# Run a test command; if it fails, repeat its failure lines as GitHub
# annotations, which show on the pull request without opening the log.
#   ci/annotate.sh TITLE COMMAND [ARGS...]
title=$1; shift
log=$(mktemp)
"$@" 2>&1 | tee "$log"
rc=${PIPESTATUS[0]}
if [ "$rc" -ne 0 ]; then
  grep -E "FAIL|EXCEPTION|Traceback|Error|error x|panicked" "$log" | head -n 10 |
    while IFS= read -r line; do
      printf '::error title=%s::%s\n' "$title" "$(printf '%s' "$line" | sed 's/%/%25/g' | cut -c1-500)"
    done
fi
rm -f "$log"
exit "$rc"
