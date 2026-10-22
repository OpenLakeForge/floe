#!/usr/bin/env bash
# G1 (#597): run load.py in a container with a hard memory limit, then report OOM state.
# usage: g1.sh <site-packages dir> <csv> <table> <mode> <path> [memory, default 4g]
# The site-packages dir is a host venv with the pinned deps (see README); the container only
# supplies a matching CPython (python:3.11-slim) so the run needs no network beyond the harness.
set -euo pipefail
site=$(realpath "$1"); csv=$(realpath "$2"); table=$3; mode=$4; path=$5; mem=${6:-4g}
here=$(dirname "$(realpath "$0")")
name="s1-g1-$table"
state=$(mktemp -d)
docker rm -f "$name" >/dev/null 2>&1 || true
# Drop the host page cache so the container's cgroup is charged for the CSV pages it reads.
sync; echo 3 > /proc/sys/vm/drop_caches || echo "warning: could not drop the page cache (needs root)"
set +e
docker run --name "$name" --memory="$mem" --memory-swap="$mem" --network host \
  -v "$site:/site:ro" -v "$here:/s1:ro" -v "$(dirname "$csv"):/data:ro" -v "$state:/state" \
  -e PYTHONPATH=/site -w /s1 python:3.11-slim \
  python load.py --csv "/data/$(basename "$csv")" --table "$table" --mode "$mode" --path "$path" \
  --run-id "g1-$table" --state-dir /state --stats-out /state/stats.jsonl
code=$?
set -e
echo "exit_code=$code oom_killed=$(docker inspect -f '{{.State.OOMKilled}}' "$name") memory_limit=$mem"
cat "$state/stats.jsonl" 2>/dev/null || true
docker rm "$name" >/dev/null
exit "$code"
