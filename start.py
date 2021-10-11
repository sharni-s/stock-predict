import paho.mqtt.client as paho
import os
import time

def on_subscribe(client, userdata, mid, granted_qos):
    #print("Subscribed to : /startForecast")
    print("Waiting for request to start forecasting...")

def on_message(client, userdata, msg):  
    numNodes = 3
    os.system('python3 prediction_publisher.py &')
    time.sleep(0.5)
    for i in range(1, numNodes + 1):
        os.system('python3 predictor_node.py ' + str(i) + ' &') 
    time.sleep(1)
    os.system('python3 stock_producer.py') 
    print("All processes started")
    

client = paho.Client()
client.on_subscribe = on_subscribe
client.on_message = on_message
client.connect("broker.hivemq.com", 1883)
client.subscribe("/startForecast", qos=1)

client.loop_forever()
