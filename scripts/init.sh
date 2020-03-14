#!/bin/bash
cd /app/dynamoplus/serverless
echo "installing python dependencies"
pip install -r requirements.txt
#pip freeze > requirements.txt
echo "installing node dependencies"
npm install
cp secrets-example.json secrets.json
cat secrets.json
