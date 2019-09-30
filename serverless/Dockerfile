FROM python:3.7

# update apt-get
RUN apt-get update -y && apt-get upgrade -y

# Install Nodejs 8
RUN curl -sL https://deb.nodesource.com/setup_8.x | bash -
RUN apt-get install -y nodejs npm


RUN node --version
RUN npm --version
# install dev tool
RUN apt-get install -y vim

# install aws-cli
RUN pip install awscli
RUN npm cache clean --force

# install serverless framework
RUN npm install -g serverless
# change work directory
RUN mkdir -p /app
WORKDIR /app
RUN cd /app
RUN npm install --save serverless-python-requirements serverless-dynamodb-local serverless-offline
RUN pip install boto3
RUN pip install jsonschema
RUN pip freeze > requirements.txt
RUN export PYTHONPATH=/app/:$PYTHONPATH