#!/bin/bash

cd /app/dynamoplus/serverless || exit
export PYTHONPATH=/app/:$PYTHONPATH
cat serverless.yml
echo "$ENVIRONMENT"
sls deploy --stage="$ENVIRONMENT"