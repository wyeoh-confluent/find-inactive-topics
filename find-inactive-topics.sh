#!/usr/bin/env bash
# Find topics with no messages produced in the last LOOKBACK_DAYS days.
#
# Runs two cluster-wide kafka-get-offsets commands (latest + lookback timestamp),
# writes interim files, then lists topics where every partition has zero growth.
# Topics whose names start with "_" are excluded from the result.
#
# Usage (from this directory):
#   ./find-inactive-topics.sh --bootstrap HOST:PORT --config FILE
#   ./find-inactive-topics.sh --lookback-days 30 --bootstrap HOST:PORT --config FILE \
#     --latest-file latest.txt --ago-file ago.txt --out-file inactive-topics.txt
#
# Requires kafka-get-offsets on PATH and a valid --command-config properties file.

set -euo pipefail

LOOKBACK_DAYS=30
BOOTSTRAP=""
CONFIG=""
LATEST_FILE="latest.txt"
AGO_FILE="ago.txt"
OUT_FILE="inactive-topics.txt"

usage() {
  cat <<EOF
Usage: $(basename "$0") --bootstrap HOST --config FILE [options]

Required:
  --bootstrap HOST    Kafka bootstrap server
  --config FILE       Admin client properties file

Options:
  --lookback-days N   Days of inactivity to check (default: ${LOOKBACK_DAYS})
  --latest-file FILE  Interim latest offsets output (default: ${LATEST_FILE})
  --ago-file FILE     Interim lookback offsets output (default: ${AGO_FILE})
  --out-file FILE     Inactive topic list output (default: ${OUT_FILE})
  -h, --help          Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lookback-days)
      LOOKBACK_DAYS="$2"
      shift 2
      ;;
    --bootstrap)
      BOOTSTRAP="$2"
      shift 2
      ;;
    --config)
      CONFIG="$2"
      shift 2
      ;;
    --latest-file)
      LATEST_FILE="$2"
      shift 2
      ;;
    --ago-file)
      AGO_FILE="$2"
      shift 2
      ;;
    --out-file)
      OUT_FILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$BOOTSTRAP" ]]; then
  echo "error: --bootstrap is required" >&2
  usage >&2
  exit 1
fi

if [[ -z "$CONFIG" ]]; then
  echo "error: --config is required" >&2
  usage >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v kafka-get-offsets >/dev/null 2>&1; then
  echo "error: kafka-get-offsets not found on PATH" >&2
  exit 1
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "error: config file not found: $CONFIG" >&2
  exit 1
fi

AGO_MS="$(python3 -c "import time; print(int((time.time() - ${LOOKBACK_DAYS} * 86400) * 1000))")"

echo "Lookback: ${LOOKBACK_DAYS} days (timestamp ms: ${AGO_MS})"
echo "Fetching latest offsets → ${LATEST_FILE}"
kafka-get-offsets \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CONFIG" \
  --exclude-internal-topics \
  --time latest \
  >"$LATEST_FILE"

echo "Fetching offsets at lookback → ${AGO_FILE}"
kafka-get-offsets \
  --bootstrap-server "$BOOTSTRAP" \
  --command-config "$CONFIG" \
  --exclude-internal-topics \
  --time "$AGO_MS" \
  >"$AGO_FILE"

echo "Comparing offsets (skipping topics starting with '_')..."
python3 "$SCRIPT_DIR/compare-inactive-offsets.py" "$LATEST_FILE" "$AGO_FILE" "$OUT_FILE"
