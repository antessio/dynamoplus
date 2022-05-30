#!/bin/bash
cd /app/dynamoplus/serverless
echo "installing node dependencies"
npm install

echo "installing python dependencies"
python -m venv venv
source veng/bin/activate
pip install -r requirements.txt

cp secrets-example.json secrets.json
