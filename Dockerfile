FROM python:3.7

# update apt-get
RUN apt-get update -y && apt-get upgrade -y

# Install Nodejs
RUN apt-get install -y npm nodejs

# install dev tool
RUN apt-get install -y vim git tree jq

# install aws-cli
#RUN pip install awscli

# install serverless framework
RUN npm install -g serverless

# change work directory
RUN mkdir -p /app

RUN nodejs --version
RUN serverless --version
ADD serverless /app/dynamoplus/serverless
ADD scripts /app/scripts

RUN chmod -R 777 /app/*
RUN /app/scripts/init.sh

WORKDIR /app

ENTRYPOINT /app/scripts/entrypoint.sh