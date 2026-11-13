#!/usr/bin/env python3
"""Compare latest vs lookback kafka-get-offsets outputs; list inactive topics."""

import sys
from collections import defaultdict


def load_offsets(path):
    # topic -> {partition: offset}
    data = defaultdict(dict)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Format: topic:partition:offset
            topic, partition, offset = line.rsplit(":", 2)
            data[topic][int(partition)] = int(offset)
    return data


def main():
    if len(sys.argv) != 4:
        print(
            f"Usage: {sys.argv[0]} <latest.txt> <ago.txt> <inactive-topics.txt>",
            file=sys.stderr,
        )
        sys.exit(1)

    latest_path, ago_path, out_path = sys.argv[1:4]
    latest = load_offsets(latest_path)
    ago = load_offsets(ago_path)

    inactive = []
    for topic, parts in sorted(latest.items()):
        if topic.startswith("_"):
            continue
        ago_parts = ago.get(topic, {})
        all_zero = True
        for partition, latest_off in parts.items():
            if partition in ago_parts:
                delta = latest_off - ago_parts[partition]
            else:
                # No offset returned for lookback timestamp ⇒ no produce in window
                delta = 0
            if delta != 0:
                all_zero = False
                break
        if all_zero:
            inactive.append(topic)

    with open(out_path, "w") as out:
        for topic in inactive:
            out.write(topic + "\n")

    print(f"Inactive topics ({len(inactive)}) written to {out_path}:")
    for topic in inactive:
        print(topic)


if __name__ == "__main__":
    main()
