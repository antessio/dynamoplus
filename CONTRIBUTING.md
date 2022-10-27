# Contributing to the project

(Draft)

## Configure the development environment

### Requirements
- Python 3
- Docker
- Node JS 14 (needed to use Serverless Framework plugins )
- Servleress Framework (https://www.serverless.com/framework/docs/getting-started)


```
sudo yum install docker git
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.1/install.sh | bash
nvm install v14.18.2


```

### Configuration

Run the following commands in the project root

```
python -m venv venv
source veng/bin/activate
cd serverless
pip install -r requirements.txt
npm install
npm install -g serverless
```

Issue with pycripto

On centos:

```
sudo yum install python3-devel
```

On ubuntu:

```
sudo apt-get install python3-devel
```



## Deploy on localhost

First run dynamo db locally:

```
docker-compose -f ./docker-compose-localhost-dynamo-only.yml
```

Then run the serverless application:

```
cd serverless
cp secrets-example.json secrets.json
## update secrets.json
vi secrets.json

## run serverless
sls offline --stage=local --noPrependStageInUrl
```

```
 curl -X POST -H 'Authorization: Basic base64(root:password)' http://localhost:3000/admin/setup
```

To check that everything works:

```
curl http://localhost:3000/system/info
```

## Deploy on AWS

```
cd serverless
cp secrets-example.json secrets.json
## update secrets.json
vi secrets.json

sls deploy --stage=dev
```
