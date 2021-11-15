#!/bin/bash
/app/scripts/setup.sh

cd /app/dynamoplus/serverless
export PYTHONPATH=/app/:$PYTHONPATH
INTEGRATION_TEST_FLAG=true sls offline --stage=local --noPrependStageInUrl