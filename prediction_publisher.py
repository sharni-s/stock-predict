import json
import time
import paho.mqtt.client as mqtt  
from datetime import datetime 
from datetime import timedelta
from kafka import KafkaConsumer


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag=True #set flag
        print("FROM MQTT PUBLISHER: Connected to MQTT broker")
        return 0
    else:
        print("FROM MQTT PUBLISHER: Bad Connection Returned code=", rc)
        return -1

mqtt.Client.connected_flag=False # create flag in class

broker = "broker.hivemq.com"
port = 1883

client = mqtt.Client("publisher")  
client.on_connect = on_connect  
client.loop_start()

try:
    client.connect(broker, port) 
except:
    #print("Connection failed")
    exit(1) 
    
while not client.connected_flag: 
    #print("In Wait loop")
    time.sleep(1)



# Consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset = 'earliest', 
              value_deserializer = lambda m: json.loads(m.decode('utf-8')),
              enable_auto_commit = False) 

              
# To consume latest messages from the topic
consumer = KafkaConsumer('stockPrediction', bootstrap_servers=['localhost:9092'])

numRecv = dict()
all_rmse = dict()
numNodes = 3

def findBest(rmse_vals):
    minIndex = 0
    for i in range(1, numNodes):
        if rmse_vals[minIndex]['rmse'] > rmse_vals[i]['rmse']:
            minIndex = i
    return minIndex        

for message in consumer:
    recvJSON = json.loads(message.value)
    if recvJSON['timestamp'] in numRecv:
        numRecv[recvJSON['timestamp']] = numRecv[recvJSON['timestamp']] + 1
        all_rmse[recvJSON['timestamp']].append(recvJSON)
        
        if (numRecv[recvJSON['timestamp']] == numNodes):
            minIndex = findBest(all_rmse[recvJSON['timestamp']])
            curr_timestamp = datetime.fromisoformat(all_rmse[recvJSON['timestamp']][minIndex]['timestamp'])
            next_timestamp = datetime.fromisoformat(all_rmse[recvJSON['timestamp']][minIndex]['next_timestamp'])    
            _message = {
                "curr_timestamp": str(curr_timestamp),
                "curr_stockval": all_rmse[recvJSON['timestamp']][minIndex]['curStockPrice'],
                "next_timestamp": str(next_timestamp),
                "next_stockval": round(all_rmse[recvJSON['timestamp']][minIndex]['predStockPrice'], 3)
            }
            mqtt_message = json.dumps(_message)
            client.publish("/stockData/tesla", mqtt_message, qos = 1)
            
    else:
        numRecv[recvJSON['timestamp']] = 1
        all_rmse[recvJSON['timestamp']] = [recvJSON]
        
            