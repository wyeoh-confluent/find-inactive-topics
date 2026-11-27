---
glean_provenance: {"generation_id":"0d3341a9a1744492a0c5c09c9c7ade68.2","generated_at":"2026-11-27T02:07:47Z","model":"Glean Assistant 571.123"}
---
# Confluent Cloud Topic Cleanup

A safety-first Bash script for deleting a reviewed list of inactive topics from a Confluent Cloud Kafka cluster using the Confluent CLI. It reads exact topic names from a file, checks them against the live cluster, previews everything by default, and only deletes after explicit confirmation.

> **Warning:** Topic deletion is irreversible and removes all data in the topic. Always run the preview first and get sign-off from topic owners.

## Contents


| File                                       | Purpose                                                              |
| ------------------------------------------ | -------------------------------------------------------------------- |
| `confluent-cloud-topic-cleanup.sh`         | The cleanup script                                                   |
| `inactive-topics.txt`                      | Topic names to delete, one exact name per line                       |
| `topic-backup-<cluster>-<timestamp>/`      | Metadata backup created by `--backup-only` or `--execute`            |
| `topic-deletion-<cluster>-<timestamp>.log` | Deletion log, saved inside the backup folder on each `--execute` run |


## Prerequisites

* macOS or Linux with Bash 3.2 or later (the default macOS `/bin/bash` works)
* [Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html) installed and on your `PATH`
* A Confluent Cloud user or service account with permission to list and delete topics on the target cluster (for example, CloudClusterAdmin or a role binding granting topic delete)
* The target Kafka cluster ID (`lkc-...`)

## Quick start

```bash
# 1. Make the script executable (downloaded files lose this permission)
chmod +x ./confluent-cloud-topic-cleanup.sh
xattr -d com.apple.quarantine ./confluent-cloud-topic-cleanup.sh 2>/dev/null   # macOS only

# 2. Log in and select the environment
confluent login
confluent environment list
confluent environment use <env-id>
confluent kafka cluster list

# 3. Preview (nothing is deleted)
CC_KAFKA_CLUSTER_ID=<lkc-id> ./confluent-cloud-topic-cleanup.sh --file ../inactive-topics.txt

# 4. Optional: save topic settings, ACLs and schemas without deleting anything
CC_KAFKA_CLUSTER_ID=<lkc-id> ./confluent-cloud-topic-cleanup.sh --file ../inactive-topics.txt --backup-only

# 5. Back up metadata, then delete, after reviewing the preview
CC_KAFKA_CLUSTER_ID=<lkc-id> ./confluent-cloud-topic-cleanup.sh --file ../inactive-topics.txt --execute
```

If you prefer not to change permissions, run it with `bash ./confluent-cloud-topic-cleanup.sh ...` instead.

## Options


| Option                | Description                                                                                                    |
| --------------------- | -------------------------------------------------------------------------------------------------------------- |
| `--file <path>`       | Topic list file. Default: `./inactive-topics.txt`                                                              |
| `--execute`           | Back up metadata, then delete. Without it, the script only previews.                                           |
| `--backup-only`       | Back up metadata for the candidates without deleting anything.                                                 |
| `--backup-dir <path>` | Backup folder. Default: `./topic-backup-<cluster>-<timestamp>`. Must not already exist.                        |
| `--skip-backup`       | With `--execute`, delete without a metadata backup. Not recommended.                                           |
| `--allow-unmarked`    | Also delete topics whose names have no `DEV`, `SIT`, or `UAT` marker. Use only after manual review.            |
| `--no-cli-prompt`     | Passes `--force` to the CLI so it does not prompt for each topic. The script's own confirmation still applies. |
| `-h`, `--help`        | Show usage                                                                                                     |



| Environment variable  | Description                                                    |
| --------------------- | -------------------------------------------------------------- |
| `CC_KAFKA_CLUSTER_ID` | Required. Target Confluent Cloud Kafka cluster ID (`lkc-...`). |


## Topic file format

* One exact topic name per line (names are case-sensitive)
* Blank lines and lines starting with `#` are ignored
* Leading/trailing whitespace and Windows line endings (CRLF) are trimmed
* Duplicates are ignored with a warning

```text
# SIT topics
MICROSERVICE.SIT.CIS.ACCOUNT.INFO
INFRA.SIT.LOGS

# Excluded pending owner confirmation
# web.orders
```

## How it works

1. **Validate input.** The script stops if the cluster ID is missing, the CLI is not installed, the file is unreadable or empty, a name contains invalid characters, or a name starts with `_` (internal topics such as `__consumer_offsets` or `_schemas`).
2. **Check the live cluster.** It runs `confluent kafka topic list` and sorts each requested topic into one of three groups:
   
   * **Will delete:** exists in the cluster and contains a `DEV`, `SIT`, or `UAT` marker
   * **Not in cluster:** listed in the file but not found; skipped
   * **No env marker:** exists but has no `DEV`, `SIT`, or `UAT` marker; held back unless `--allow-unmarked` is set
3. **Preview.** It prints the counts and every topic in each group, then exits unless `--execute` was passed.
4. **Confirm.** With `--execute`, you must type the cluster ID exactly, then `DELETE <count>` (for example `DELETE 128`).
5. **Back up metadata.** Before any deletion, the script saves the settings for every candidate topic (see Metadata backup below). If the description or configuration of any topic cannot be saved, it stops and deletes nothing.
6. **Delete.** Each topic is deleted with `confluent kafka topic delete <topic> --cluster <lkc-id>`. Unless `--no-cli-prompt` is used, the CLI also asks for confirmation for each topic.
7. **Log and summarize.** Output is written to a timestamped log, and the script reports how many deletions were submitted and how many failed or were declined. It exits non-zero if any failed.

Topics in the cluster that are not in your file are never touched.

## Metadata backup

Runs with `--backup-only`, and automatically with `--execute` (unless `--skip-backup`). It does not back up message data; see the cleanup procedure below for options.


| Path in backup folder                                      | Contents                                                             | Source command                                                                             |
| ---------------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `topics/<topic>.json`                                      | Partitions and topic details                                         | `confluent kafka topic describe <topic> -o json`                                           |
| `configs/<topic>.json`                                     | Topic configuration, such as retention and cleanup policy            | `confluent kafka topic configuration list <topic> -o json`                                 |
| `acls/<topic>.json`                                        | ACLs on the topic                                                    | `confluent kafka acl list --topic <topic> -o json`                                         |
| `schemas/<topic>-key.txt`, `<topic>-value.txt`             | Latest schema version, if the subject exists                         | `confluent schema-registry schema describe --subject <topic>-<key/value> --version latest` |
| `manifest.tsv`                                             | Per-topic result: `ok`, `FAIL` or `none` for each item               | &nbsp;                                                                                     |
| `errors.log`                                               | CLI errors, only kept if something failed                            | &nbsp;                                                                                     |
| `candidates.txt`, `topic-list-used.txt`, `backup-info.txt` | What was processed, the input file, cluster ID, time and CLI version | &nbsp;                                                                                     |


Notes:

* Failing to save a topic's description or configuration blocks deletion. ACL export failures (often a permissions issue) are reported but do not block it.
* Schemas are looked up using the default `<topic>-key` and `<topic>-value` subject names. Subjects that use other naming strategies are not captured; back those up separately.
* Only the latest schema version is saved. Schema subjects are not deleted by this script.
* RBAC role bindings are not exported.

## Current topic list

`inactive-topics.txt` contains 133 unique topic names across DEV, SIT, and UAT environments.

Three names have no environment marker and are excluded by default:


| Topic                 | Note                                                                                      |
| --------------------- | ----------------------------------------------------------------------------------------- |
| `dlq-lcc-3om0go`      | Looks like a connector dead letter queue. Confirm the related connector has been deleted. |
| `web.internal.orders` | Could be production. Confirm with the owner.                                              |
| `web.orders`          | Could be production. Confirm with the owner.                                              |


To include them after review, add `--allow-unmarked`. To keep them, remove them from the file or comment them out with `#`.

## Recommended cleanup procedure

### Before deletion

* Confirm each topic is unused: no active producers or consumers, connectors, Flink statements, ksqlDB queries, or Cluster Link mirrors.
* Check bytes in/out metrics and consumer group lag in the Confluent Cloud Console or Metrics API.
* Get sign-off from the application or topic owners, especially for topics without an environment marker.
* Export or back up any message data that must be kept. The script saves metadata only.
* Make sure Terraform, CI/CD pipelines, or applications that create topics automatically will not recreate the topics. Remove them from infrastructure code first.
* Run the preview and save its output for change records.
* Optionally run `--backup-only` ahead of the change window and review `manifest.tsv`.

### During deletion

* Run during an agreed change window.
* Consider doing it in batches (for example DEV first, then SIT, then UAT) by splitting the topic file.
* Review any `FAIL` lines in the output.

### After deletion

* Verify with `confluent kafka topic list --cluster <lkc-id>`. Deletion can take time to complete, and a topic with the same name cannot be recreated until it finishes.
* Clean up related resources if they are no longer needed:
  
  * Schema Registry subjects (for example `<topic>-key` and `<topic>-value`)
  * ACLs and RBAC role bindings that reference the topics
  * Stale consumer groups
  * Dashboards, alerts, and documentation that refer to the topics
* Keep the backup folder, including the deletion log, as evidence of the change.

## Troubleshooting


| Symptom                                        | Fix                                                                                                                                                             |
| ---------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `zsh: permission denied`                       | Run `chmod +x ./confluent-cloud-topic-cleanup.sh`, or run with `bash ./confluent-cloud-topic-cleanup.sh`                                                        |
| `Confluent CLI ('confluent') is not installed` | Install the Confluent CLI and make sure `confluent` is on your `PATH`                                                                                           |
| `Could not list topics for cluster`            | Run `confluent login`, select the right environment with `confluent environment use`, and check the cluster ID and permissions                                  |
| Everything appears under "Not in cluster"      | Wrong environment or cluster selected, or names do not match exactly (case-sensitive)                                                                           |
| `Confirmation did not match; aborting.`        | Type the cluster ID exactly, then `DELETE` followed by the exact count shown                                                                                    |
| `ABORTED: settings could not be saved`         | Check `errors.log` and `manifest.tsv` in the backup folder, fix the cause (usually permissions or the wrong environment), then re-run with a new `--backup-dir` |
| `Backup folder already exists`                 | Use a different `--backup-dir`, or remove the old folder                                                                                                        |
| `FAIL <topic>` in the summary                  | Check the log for the CLI error, usually permissions or a topic that was already deleted                                                                        |


## References

* [confluent kafka topic delete](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_delete.html)
* [confluent kafka topic list](https://docs.confluent.io/confluent-cli/current/command-reference/kafka/topic/confluent_kafka_topic_list.html)
* [Manage topics in Confluent Cloud](https://docs.confluent.io/cloud/current/client-apps/topics/manage.html)

*Co-authored with AI from Glean · gen:0d3341a9a1744492a0c5c09c9c7ade68.2 · 2026-11-27T02:07:47Z*