#!/usr/bin/env bash
# Produce a small keyed changelog to the ecomm.v1.clickstream topic for the
# upsert-kafka CDC demo (sql/06). The upsert-kafka connector treats a null value
# as a tombstone (DELETE), and the message key is the JSON-encoded primary key.
#
# Final expected table state after this changelog:
#   user 1 -> status=active, amount=20   (inserted then updated)
#   user 2 -> DELETED                     (inserted then tombstoned)
#   user 3 -> status=new,    amount=7     (inserted)
#
# Runs rpk INSIDE the redpanda container (via docker compose exec) so it works
# regardless of host tooling. Uses the internal listener redpanda:29092.
set -euo pipefail

TOPIC="ecomm.v1.clickstream"
BROKERS="redpanda:29092"

# rpk reads records as  key<TAB>value  with --format '%k\t%v\n'.
# A trailing tab with empty value produces a tombstone (null value) = DELETE.
read -r -d '' RECORDS <<'EOF' || true
{"user_id":1}	{"user_id":1,"status":"new","amount":10.0}
{"user_id":2}	{"user_id":2,"status":"new","amount":5.0}
{"user_id":3}	{"user_id":3,"status":"new","amount":7.0}
{"user_id":1}	{"user_id":1,"status":"active","amount":20.0}
{"user_id":2}	
EOF

echo "Producing changelog to ${TOPIC} (via redpanda:29092):"
printf '%s\n' "$RECORDS"

printf '%s\n' "$RECORDS" | docker compose exec -T redpanda \
  rpk topic produce "$TOPIC" --brokers "$BROKERS" --format '%k\t%v\n'

echo "Done. Now submit sql/06 (just demo-upsert-kafka does produce + submit + verify)."
