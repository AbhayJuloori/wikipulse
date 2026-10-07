#!/bin/bash
set -euo pipefail
for topic in wiki.edits.raw wiki.edits.dlq; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 1
done
