#!/bin/bash

cd /app/dynamoplus/serverless
export PYTHONPATH=/app/:$PYTHONPATH
cat serverless.yml
echo "$ENVIRONMENT"
sls deploy --stage="$ENVIRONMENT"