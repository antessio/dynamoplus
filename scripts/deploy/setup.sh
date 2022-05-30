#!/bin/bash
cd /app/dynamoplus/serverless
echo "installing node dependencies"
npm install

sls plugin install -n serverless-python-requirements

echo "installing python dependencies"
python -m venv venv
source veng/bin/activate
pip install -r requirements.txt


echo "setting secrets"
if [[ -z "${ROOT_PASSWORD}" ]]; then

  DYNAMOPLUS_ROOT_PASSWORD=$(python /app/scripts/random_string.py)
else
  DYNAMOPLUS_ROOT_PASSWORD="${ROOT_PASSWORD}"
fi

if [[ -z "${SERVICE_NAME}" ]]; then

  DYNAMOPLUS_SERVICE_NAME="dynamoplus"
else
  DYNAMOPLUS_SERVICE_NAME="${SERVICE_NAME}"
fi

if [[ -z "${DYNAMODB_HOST}" ]]; then

  DYNAMODB_HOST="http://dynamolocal"
fi
if [[ -z "${DYNAMODB_PORT}" ]]; then

  DYNAMODB_PORT="8080"
fi

echo "service name: $DYNAMOPLUS_SERVICE_NAME"
echo "dynamodb  host: $DYNAMODB_HOST"

sed "s/passwordtochangeinproduction/$DYNAMOPLUS_ROOT_PASSWORD/g" secrets-example.json > secrets.json
sed -i "s/service: dynamoplus/service: $DYNAMOPLUS_SERVICE_NAME/g" serverless.yml
sed -i 's/dockerizePip: true/dockerizePip: false/g' serverless.yml
sed -i -E "s@(DYNAMODB_HOST:).*@\1 $DYNAMODB_HOST@" serverless.yml
sed -i -E "s@(DYNAMODB_PORT:).*@\1 $DYNAMODB_PORT@" serverless.yml

cat serverless.yml
