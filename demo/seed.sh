#!/usr/bin/env bash
# Seeds a local Kafka broker with the Acme Shop demo topology.
#
# Idempotent: topics are created with --if-not-exists, messages are only produced into empty
# topics, and committed offsets are (re)set for every group. Safe to run repeatedly.
# Resetting offsets requires each group to have no active members, so do not point the seeder at
# a cluster where those groups are live.
#
# Runs inside the apache/kafka image (see docker-compose.yml) but also works on a host with the
# Kafka CLI tools if KAFKA_BIN and KAFKA_BOOTSTRAP_SERVERS are set.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
BIN="${KAFKA_BIN:-/opt/kafka/bin}"
DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MESSAGES_PER_TOPIC="${MESSAGES_PER_TOPIC:-12}"

is_comment_or_blank() {
  [[ -z "$1" || "$1" == \#* ]]
}

echo "Waiting for broker at $BOOTSTRAP ..."
for attempt in $(seq 1 60); do
  if "$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    echo "Broker is up."
    break
  fi
  if [[ "$attempt" -eq 60 ]]; then
    echo "Broker not reachable after 60 attempts" >&2
    exit 1
  fi
  sleep 2
done

echo "Creating topics ..."
while read -r topic partitions; do
  topic="${topic%$'\r'}"
  partitions="${partitions%$'\r'}"
  is_comment_or_blank "$topic" && continue
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 >/dev/null
  echo "  topic   $topic ($partitions partition(s))"
done < "$DEMO_DIR/topics.txt"

echo "Producing sample messages ..."
while read -r topic partitions; do
  topic="${topic%$'\r'}"
  partitions="${partitions%$'\r'}"
  is_comment_or_blank "$topic" && continue
  case "$topic" in
    *-changelog|*-repartition) continue ;;
  esac
  existing=$("$BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$topic" \
    --time -1 | awk -F: '{ sum += $NF } END { print sum + 0 }')
  if [[ "$existing" -gt 0 ]]; then
    echo "  skip    $topic already holds $existing message(s)"
    continue
  fi
  for n in $(seq 1 "$MESSAGES_PER_TOPIC"); do
    printf '{"eventId":"%s-%04d","sequence":%d,"source":"acme-shop-demo-seeder"}\n' \
      "$topic" "$n" "$n"
  done | "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOTSTRAP" \
    --topic "$topic" >/dev/null
  echo "  produce $topic <- $MESSAGES_PER_TOPIC message(s)"
done < "$DEMO_DIR/topics.txt"

echo "Committing consumer-group offsets ..."
while read -r group topics; do
  group="${group%$'\r'}"
  topics="${topics%$'\r'}"
  is_comment_or_blank "$group" && continue
  IFS=',' read -ra topic_list <<< "$topics"
  for t in "${topic_list[@]}"; do
    "$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --group "$group" \
      --topic "$t" --reset-offsets --to-latest --execute >/dev/null
  done
  echo "  group   $group -> $topics"
done < "$DEMO_DIR/groups.txt"

echo
echo "Seed complete. Consumer groups on the broker:"
"$BIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOTSTRAP" --list | sort | sed 's/^/  /'
