#!/usr/bin/env bash
URL=$1
MAX_TRIES=$2
SECONDS_BETWEEN_CHECKS=$3
echo "URL: $URL"
echo "MAX_TRIES: $MAX_TRIES"
echo "SECONDS_BETWEEN_CHECKS: $SECONDS_BETWEEN_CHECKS"
while [ $MAX_TRIES -gt 0 ]
do
  STATUS=$(curl -L --max-time 1 -s -o /dev/null -w '%{http_code}' $URL)
  echo "Status $STATUS"
  if [ $STATUS -eq 200 ]; then
    exit 0
  else
    MAX_TRIES=$((MAX_TRIES - 1))
  fi
  echo "attempt left $MAX_TRIES"
  sleep $SECONDS_BETWEEN_CHECKS
done