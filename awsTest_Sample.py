'''
Send Time Sample
'''

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import json
import datetime
import logging

# Setup Param　　※AWS IoTのエンドポイントを指定
host = "xxxxxxxxxxxxxxxxxxxxxxxxxxx.amazonaws.com"

# for Raspberry Pi 　　　※AWS IoTの証明書
rootCAPath = "AmazonRootCA1.pem"
certificatePath = "xxxxxxxxxxxxxxxxxxxxxxxx-certificate.pem.crt"
privateKeyPath = "xxxxxxxxxxxxxxxxxxxxxxxx-private.pem.key"

useWebsocket = False
clientId = "testRule"
topic = "testRule/time"
deviceNo = "0301a279f9b9"

# Configure logging
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Init AWSIoTMQTTClient
myAWSIoTMQTTClient = None
myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureEndpoint(host, 8883)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)      # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)            # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)    # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)         # 5 sec

# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()
# Initial
time.sleep(2)

try:
    while True:
        nowtime = datetime.datetime.now()
        nowsettime = int(time.mktime(nowtime.timetuple()))  # UNIX Time
        #send Command
        message = {}
        message['time'] = nowtime.strftime('%Y/%m/%d %H:%M:%S')
        message['deviceNo'] = deviceNo
        f = open("/sys/bus/w1/devices/28-031868f200ff/w1_slave", "r")
        line = f .readlines()[1].replace("\n", "")
        n = int(line.find("t="))
        message['temperature']  = int(line[n+2:]) * 0.001 
        f.close()
        messageJson = json.dumps(message)
        print (messageJson)
        myAWSIoTMQTTClient.publish(topic, messageJson, 1)
        time.sleep(60)
except:
    import traceback
    traceback.print_exc()

print ("Terminated")
