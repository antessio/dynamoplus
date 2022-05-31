FROM python:3.7

# update apt-get
RUN apt-get update -y && apt-get upgrade -y

# Install Nodejs
RUN apt-get install -y npm nodejs

# install dev tool
RUN apt-get install -y vim git tree jq


# install serverless framework
RUN npm install -g serverless

# change work directory
RUN mkdir -p /app

RUN nodejs --version
RUN serverless --version
ADD ../../serverless /app/dynamoplus/serverless
ADD ../../scripts /app/scripts

RUN chmod -R 777 /app/*
ARG AWS_ACCESS_KEY_ID
ENV AWS_ACCESS_KEY_ID $AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ENV AWS_SECRET_ACCESS_KEY $AWS_SECRET_ACCESS_KEY
ARG ROOT_PASSWORD
ENV ROOT_PASSWORD $ROOT_PASSWORD
ARG SERVICE_NAME
ENV SERVICE_NAME $SERVICE_NAME

RUN /app/scripts/deploy/setup.sh

WORKDIR /app

ENTRYPOINT /app/scripts/deploy/deploy.sh