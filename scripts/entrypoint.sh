#!/bin/bash
/app/scripts/setup.sh

cd /app/dynamoplus/serverless
export PYTHONPATH=/app/:$PYTHONPATH
#sls --stage local dynamodb migrate --port=8000 --host=dynamolocal
INTEGRATION_TEST_FLAG=true sls offline --stage=local