import os
import requests
import json
from aws_requests_auth.aws_auth import AWSRequestsAuth
import string
import datetime
from urllib.parse import urlparse, parse_qs

def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] != 'INSERT':
            return
        strTime = (record['dynamodb']['NewImage']['time']['S'])
        strDevice = (record['dynamodb']['NewImage']['deviceNo']['S'])
        strTemperature = (record['dynamodb']['NewImage']['temperature']['N'])
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")

    auth = AWSRequestsAuth(aws_access_key= os.environ.get('AWS_ACCESS_KEY_ID'),
                           aws_secret_access_key= os.environ.get('AWS_SECRET_ACCESS_KEY'),
                           aws_token = os.environ.get('AWS_SESSION_TOKEN'),
                           aws_host='xxxxxxxxxxxxxxxxxxxxxxxxx.es.amazonaws.com',  ※OpenSearch Serviceのエンドポイント
                           aws_region='ap-northeast-1',　※AWSのリージョン
                           aws_service='es')                   
    url = ("https://xxxxxxxxxxxxxxxxxx.es.amazonaws.com/_dashboards/")　※OenSeach SeviceのDashboad
    data = json.dumps({ "time": strTime, 
                        "deviceNo": strDevice, 
                        "temperature": float(strTemperature), 
                        "@timestamp": now })
    res = requests.post(url, 
                        data=data, 
                        headers={'Content-Type':'application/json'}, 
                        auth=auth)

    return {"result": "OK"}
