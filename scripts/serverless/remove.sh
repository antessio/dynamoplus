#!/bin/bash

cd /app/dynamoplus/serverless || exit
export PYTHONPATH=/app/:$PYTHONPATH
sls remove