#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset

if [[ $# -eq 0 ]]; then
  echo "$0: <email recipients>..." >&2
  exit 1
fi

from_address=${DATAGOV_DEDUPE_FROM_ADDRESS:-'no-reply+dedupe-report@data.gov'}

# Log for this run.
dedupe_run_log=$(mktemp)
dedupe_report=$(mktemp)

report_date=$(date +%Y-%m-%d)

function cleanup () {
  rm -rf "$dedupe_report" "$dedupe_run_log"
}

trap cleanup EXIT

# Run the script, appending to dedupe.log across runs as well as this run log.
python duplicates-identifier-api.py 2>&1 | tee -a dedupe.log > "$dedupe_run_log"

# For the actual report, include errors, warnings, and non-zero summary items.
grep -E 'ERROR|WARN|Summary' < "$dedupe_run_log" | grep -v 'duplicate_count=0' > "$dedupe_report"

if [[ "$(wc -l "$dedupe_report")" == 0 ]]; then
  # Nothing in the report
  exit 0
fi

mail -a "From: $from_address" -s "[datagov-dedupe] report $report_date" "$@" <<EOF
Hello team,

This is datagov-dedupe reporting on the number of duplicate packages detected on Data.gov for $report_date.

$(cat "$dedupe_report")

--
$(basename "$0") on $(hostname)
https://github.com/GSA/datagov-dedupe
EOF
