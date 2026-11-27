#!/usr/bin/env bash
# Safe Confluent Cloud topic cleanup driven by a topic-list file.
# Compatible with macOS /bin/bash 3.2 and newer Bash versions.
#
# Usage:
#   CC_KAFKA_CLUSTER_ID=lkc-xxxxx bash ./confluent-cloud-topic-cleanup.sh [options]
#
# Options:
#   --file <path>        Topic list (one exact name per line). Default: ./inactive-topics.txt
#   --execute            Back up metadata, then delete. Without this flag the script only previews.
#   --backup-only        Back up metadata for the candidates; do not delete anything.
#   --backup-dir <path>  Backup folder. Default: ./topic-backup-<cluster>-<timestamp>
#   --skip-backup        With --execute, delete without taking a metadata backup (not recommended).
#   --allow-unmarked     Also include topics whose names have no DEV/SIT/UAT marker.
#   --no-cli-prompt      Pass --force to the CLI (skip per-topic prompts) AFTER the
#                        script's own cluster-ID + DELETE confirmation.
#
# The backup saves topic settings, per-topic ACLs and latest key/value schemas. It does
# NOT back up message data. If any topic's settings cannot be saved, nothing is deleted.
# File format: blank lines and lines starting with '#' are ignored; whitespace/CRLF trimmed.
set -o pipefail

TOPIC_FILE="./inactive-topics.txt"
EXECUTE=false
BACKUP_ONLY=false
SKIP_BACKUP=false
BACKUP_DIR=""
ALLOW_UNMARKED=false
CLI_FORCE=false
CLUSTER_ID="${CC_KAFKA_CLUSTER_ID:-}"
ENV_MARKER_REGEX='(^|[._-])(DEV|SIT|UAT)([._-]|$)'
TS="$(date +%Y%m%d-%H%M%S)"

usage() { sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
die() { echo "ERROR: $*" >&2; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --file) [ $# -ge 2 ] || usage; TOPIC_FILE="$2"; shift 2 ;;
    --execute) EXECUTE=true; shift ;;
    --backup-only) BACKUP_ONLY=true; shift ;;
    --backup-dir) [ $# -ge 2 ] || usage; BACKUP_DIR="$2"; shift 2 ;;
    --skip-backup) SKIP_BACKUP=true; shift ;;
    --allow-unmarked) ALLOW_UNMARKED=true; shift ;;
    --no-cli-prompt) CLI_FORCE=true; shift ;;
    -h|--help) usage ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

$EXECUTE && $BACKUP_ONLY && die "Use either --execute or --backup-only, not both."
$SKIP_BACKUP && ! $EXECUTE && die "--skip-backup only applies with --execute."
[ -n "$CLUSTER_ID" ] || die "Set CC_KAFKA_CLUSTER_ID to the intended Confluent Cloud Kafka cluster ID (lkc-...)."
command -v confluent >/dev/null 2>&1 || die "Confluent CLI ('confluent') is not installed or not on PATH."
[ -f "$TOPIC_FILE" ] && [ -r "$TOPIC_FILE" ] || die "Topic file not found or unreadable: $TOPIC_FILE"
[ -n "$BACKUP_DIR" ] || BACKUP_DIR="./topic-backup-${CLUSTER_ID}-${TS}"
if { $BACKUP_ONLY || { $EXECUTE && ! $SKIP_BACKUP; }; } && [ -e "$BACKUP_DIR" ]; then
  die "Backup folder already exists: $BACKUP_DIR (choose another with --backup-dir)"
fi

WORK="$(mktemp -d "${TMPDIR:-/tmp}/topic-cleanup.XXXXXX")" || die "Cannot create temp dir"
trap 'rm -rf "$WORK"' EXIT
: > "$WORK/requested"; : > "$WORK/existing"; : > "$WORK/delete"; : > "$WORK/missing"; : > "$WORK/unmarked"

count() { wc -l < "$1" | tr -d ' '; }
in_file() { grep -Fxq -- "$1" "$2"; }

# ---- Load and validate topic file ----
line_no=0
while IFS= read -r raw || [ -n "$raw" ]; do
  line_no=$((line_no + 1))
  topic="$(printf '%s' "$raw" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  [ -z "$topic" ] && continue
  case "$topic" in \#*) continue ;; esac
  [[ "$topic" =~ ^[A-Za-z0-9._-]{1,249}$ ]] || die "Line $line_no: invalid topic name: '$topic'"
  case "$topic" in _*) die "Line $line_no: refusing internal/protected-looking topic: $topic" ;; esac
  if in_file "$topic" "$WORK/requested"; then
    echo "WARN: duplicate on line $line_no ignored: $topic" >&2; continue
  fi
  printf '%s\n' "$topic" >> "$WORK/requested"
done < "$TOPIC_FILE"

[ "$(count "$WORK/requested")" -gt 0 ] || die "No topic names found in $TOPIC_FILE."

# ---- Compare against what actually exists in the cluster ----
echo "Fetching current topic list from cluster $CLUSTER_ID ..."
if ! cluster_json="$(confluent kafka topic list --cluster "$CLUSTER_ID" -o json 2>&1 </dev/null)"; then
  die "Could not list topics for cluster $CLUSTER_ID: $cluster_json"
fi
printf '%s\n' "$cluster_json" | grep -oE '"name"[[:space:]]*:[[:space:]]*"[^"]*"' \
  | sed -E 's/.*:[[:space:]]*"([^"]*)"/\1/' > "$WORK/existing"

while IFS= read -r t; do
  upper="$(printf '%s' "$t" | tr '[:lower:]' '[:upper:]')"
  if ! in_file "$t" "$WORK/existing"; then
    echo "$t" >> "$WORK/missing"
  elif ! [[ "$upper" =~ $ENV_MARKER_REGEX ]]; then
    echo "$t" >> "$WORK/unmarked"
    $ALLOW_UNMARKED && echo "$t" >> "$WORK/delete"
  else
    echo "$t" >> "$WORK/delete"
  fi
done < "$WORK/requested"

N_REQ=$(count "$WORK/requested"); N_DEL=$(count "$WORK/delete")
N_MISS=$(count "$WORK/missing"); N_UNM=$(count "$WORK/unmarked")
if $ALLOW_UNMARKED; then UNM_NOTE="INCLUDED via --allow-unmarked"; else UNM_NOTE="EXCLUDED; use --allow-unmarked after manual review"; fi

echo
echo "Target cluster : $CLUSTER_ID"
echo "Topic file     : $TOPIC_FILE ($N_REQ unique names)"
echo "Candidates     : $N_DEL"
echo "Not in cluster : $N_MISS (skipped)"
echo "No env marker  : $N_UNM ($UNM_NOTE)"
echo
if [ "$N_UNM" -gt 0 ]; then echo "Topics without DEV/SIT/UAT marker (review carefully):"; sed 's/^/  ! /' "$WORK/unmarked"; echo; fi
if [ "$N_MISS" -gt 0 ]; then echo "Listed but not found in cluster:"; sed 's/^/  ? /' "$WORK/missing"; echo; fi
if [ "$N_DEL" -gt 0 ]; then echo "Candidates (backup + deletion):"; sed 's/^/  - /' "$WORK/delete"; fi

if ! $EXECUTE && ! $BACKUP_ONLY; then
  echo
  echo "PREVIEW ONLY - nothing was backed up or deleted."
  echo "Re-run with --backup-only to save metadata, or --execute to back up and then delete."
  exit 0
fi

[ "$N_DEL" -gt 0 ] || { echo "Nothing to process."; exit 0; }

# ---- Metadata backup ----
# Returns non-zero if any topic's description or configuration could not be saved.
run_backup() {
  [ -e "$BACKUP_DIR" ] && die "Backup folder already exists: $BACKUP_DIR (choose another with --backup-dir)"
  mkdir -p "$BACKUP_DIR/topics" "$BACKUP_DIR/configs" "$BACKUP_DIR/acls" "$BACKUP_DIR/schemas" \
    || die "Cannot create backup folder: $BACKUP_DIR"
  local manifest="$BACKUP_DIR/manifest.tsv" errlog="$BACKUP_DIR/errors.log"
  cp "$TOPIC_FILE" "$BACKUP_DIR/topic-list-used.txt"
  cp "$WORK/delete" "$BACKUP_DIR/candidates.txt"
  {
    echo "cluster_id=$CLUSTER_ID"
    echo "created=$TS"
    echo "topic_file=$TOPIC_FILE"
    echo "candidates=$N_DEL"
    echo "cli_version=$(confluent version 2>/dev/null </dev/null | grep -i '^version' | head -1)"
  } > "$BACKUP_DIR/backup-info.txt"
  printf 'topic\tdescribe\tconfig\tacls\tkey_schema\tvalue_schema\n' > "$manifest"
  : > "$errlog"

  local t d c a k v s out hard_fail=0 n=0
  echo
  echo "Backing up metadata for $N_DEL topics to $BACKUP_DIR ..."
  while IFS= read -r t <&4; do
    n=$((n + 1))
    printf '  [%d/%d] %s\n' "$n" "$N_DEL" "$t"
    d=FAIL; c=FAIL; a=FAIL; k=none; v=none

    if confluent kafka topic describe "$t" --cluster "$CLUSTER_ID" -o json \
         > "$BACKUP_DIR/topics/$t.json" 2>>"$errlog" </dev/null; then d=ok
    else rm -f "$BACKUP_DIR/topics/$t.json"; echo "    describe FAILED for $t" >> "$errlog"; fi

    if confluent kafka topic configuration list "$t" --cluster "$CLUSTER_ID" -o json \
         > "$BACKUP_DIR/configs/$t.json" 2>>"$errlog" </dev/null; then c=ok
    else rm -f "$BACKUP_DIR/configs/$t.json"; echo "    configuration list FAILED for $t" >> "$errlog"; fi

    if confluent kafka acl list --cluster "$CLUSTER_ID" --topic "$t" -o json \
         > "$BACKUP_DIR/acls/$t.json" 2>>"$errlog" </dev/null; then a=ok
    else rm -f "$BACKUP_DIR/acls/$t.json"; echo "    acl list FAILED for $t (permissions?)" >> "$errlog"; fi

    for s in key value; do
      if out="$(confluent schema-registry schema describe --subject "$t-$s" --version latest 2>&1 </dev/null)"; then
        printf '%s\n' "$out" > "$BACKUP_DIR/schemas/$t-$s.txt"
        if [ "$s" = key ]; then k=ok; else v=ok; fi
      fi
    done

    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$t" "$d" "$c" "$a" "$k" "$v" >> "$manifest"
    if [ "$d" != ok ] || [ "$c" != ok ]; then hard_fail=$((hard_fail + 1)); fi
  done 4< "$WORK/delete"

  local n_acl_fail n_k n_v
  n_acl_fail=$(awk -F'\t' 'NR>1 && $4!="ok"' "$manifest" | wc -l | tr -d ' ')
  n_k=$(awk -F'\t' 'NR>1 && $5=="ok"' "$manifest" | wc -l | tr -d ' ')
  n_v=$(awk -F'\t' 'NR>1 && $6=="ok"' "$manifest" | wc -l | tr -d ' ')
  echo
  echo "Backup summary ($BACKUP_DIR/manifest.tsv):"
  echo "  Topic settings failed : $hard_fail"
  echo "  ACL export failed     : $n_acl_fail"
  echo "  Key schemas saved     : $n_k"
  echo "  Value schemas saved   : $n_v  (topics without a <topic>-key/-value subject show 'none')"
  [ -s "$errlog" ] || rm -f "$errlog"
  echo "  Message data is NOT included in this backup."
  [ "$hard_fail" -eq 0 ]
}

if $BACKUP_ONLY; then
  if run_backup; then echo "Backup complete. Nothing was deleted."; exit 0
  else echo "Backup finished with errors; see $BACKUP_DIR/errors.log" >&2; exit 1; fi
fi

# ---- Confirm deletion ----
echo
echo "WARNING: topic deletion is irreversible and removes all data in these $N_DEL topics."
$SKIP_BACKUP && echo "WARNING: --skip-backup is set; no metadata backup will be taken."
read -r -p "Type the cluster ID exactly to continue: " typed_cluster
[ "$typed_cluster" = "$CLUSTER_ID" ] || { echo "Cluster ID did not match; aborting." >&2; exit 1; }
read -r -p "Type DELETE $N_DEL to proceed: " typed_confirmation
[ "$typed_confirmation" = "DELETE $N_DEL" ] || { echo "Confirmation did not match; aborting." >&2; exit 1; }

if ! $SKIP_BACKUP; then
  if ! run_backup; then
    echo "ABORTED: settings could not be saved for one or more topics. Nothing was deleted." >&2
    echo "See $BACKUP_DIR/errors.log and $BACKUP_DIR/manifest.tsv" >&2
    exit 1
  fi
fi

LOG_FILE="topic-deletion-${CLUSTER_ID}-${TS}.log"
$SKIP_BACKUP || LOG_FILE="$BACKUP_DIR/$LOG_FILE"
echo
echo "Logging to $LOG_FILE"

ok=0; fail=0
# Read from fd 3 so the CLI's per-topic prompts still read from your terminal.
while IFS= read -r t <&3; do
  echo "Deleting: $t" | tee -a "$LOG_FILE"
  if $CLI_FORCE; then
    confluent kafka topic delete "$t" --cluster "$CLUSTER_ID" --force 2>&1 | tee -a "$LOG_FILE"
  else
    confluent kafka topic delete "$t" --cluster "$CLUSTER_ID" 2>&1 | tee -a "$LOG_FILE"
  fi
  if [ "${PIPESTATUS[0]}" -eq 0 ]; then
    ok=$((ok + 1)); echo "  OK   $t" >> "$LOG_FILE"
  else
    fail=$((fail + 1)); echo "  FAIL $t" | tee -a "$LOG_FILE"
  fi
done 3< "$WORK/delete"

echo
echo "Submitted: $ok   Failed/declined: $fail" | tee -a "$LOG_FILE"
echo "Verify with: confluent kafka topic list --cluster $CLUSTER_ID (deletion may take time to complete)."
[ "$fail" -eq 0 ]
