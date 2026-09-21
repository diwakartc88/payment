#!/bin/bash
# Creates all pipeline + DLQ topics with local-dev partition counts.
# Production partition counts are an order of magnitude+ higher -- see
# SCALING.md §2 for the sizing math. Run against the compose 'kafka' service:
#   docker compose exec kafka /scripts/create-topics.sh
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

create_topic() {
  local topic=$1
  local partitions=$2
  echo "Creating topic: $topic (partitions=$partitions)"
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms=604800000
}

create_topic payment.initiated 6
create_topic payment.fraud.checked 6
create_topic payment.authorized 6
create_topic payment.ledger.updated 6
create_topic payment.settled 6

create_topic payment.dlq.fraud 3
create_topic payment.dlq.authorization 3
create_topic payment.dlq.ledger 3
create_topic payment.dlq.settlement 3
create_topic payment.dlq.notification 3

echo "Done. Current topics:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
