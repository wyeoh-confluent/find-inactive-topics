# Find inactive Kafka topics

Finds topics that had **no messages produced** in a lookback window (default: 30 days) by comparing `kafka-get-offsets` snapshots.

A topic qualifies when `latest − lookback` is `0` for **every** partition (a missing lookback offset counts as `0`). Topics whose names start with `_` are skipped.

## Prerequisites

- `kafka-get-offsets` on `PATH` (Confluent / Kafka CLI)
- `python3`
- An Admin client properties file (SASL/SSL, etc.) for the cluster

## Usage

```bash
cd empty_topics/find-inactive-topics

./find-inactive-topics.sh \
  --bootstrap pkc-xxxx.region.aws.confluent.cloud:9092 \
  --config /path/to/confluent-kafka.properties
```

Optional flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--lookback-days` | `30` | Inactivity window in days |
| `--latest-file` | `latest.txt` | Interim latest offsets |
| `--ago-file` | `ago.txt` | Interim lookback offsets |
| `--out-file` | `inactive-topics.txt` | Result list |

Re-run comparison only (after interim files already exist):

```bash
python3 compare-inactive-offsets.py latest.txt ago.txt inactive-topics.txt
```

## How it works

1. Fetch all-topic latest offsets → `latest.txt`
2. Fetch all-topic offsets at `(now − lookback)` → `ago.txt`
3. `compare-inactive-offsets.py` lists topics with zero growth on all partitions

Offset lines look like `topic:partition:offset`.

## Caveats

- If retention is shorter than the lookback window, recently produced-then-expired messages may look like inactivity.
- Only topics visible to the API key / ACLs are included.
- `--exclude-internal-topics` drops Kafka internals; names like `confluent.connect-*` (no leading `_`) remain eligible.
