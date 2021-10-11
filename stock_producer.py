# Program for producing stock data
import time
import datetime
import pandas as pd 
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

ColumnNames = ['timestamp', 'open', 'high', 'low', 'close', 'volume']  

# Create pandas dataframe
df = pd.read_csv('TeslaStock.csv', names=ColumnNames, header=None, skiprows=1,
                     comment='#', sep=',', encoding = 'iso-8859-1')   
df_test = df[df['timestamp'] > '2021-10-07'].copy()
df_test.sort_values(by=['timestamp'], inplace=True)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

# Produce json messages
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))

for index, row in df_test.iterrows():
    # Put the values in message and publish to kafka topic
    if (df_test.shape[0] - index < 0):
        next_timestamp = curr_timestamp + timedelta(minutes = 5)
    else:
        next_timestamp = df_test.iloc[df_test.shape[0] - index]['timestamp']
    producer.send('stock', {'timestamp': row['timestamp'], 'stockPrice': row['close'], 
                  'next_timestamp': next_timestamp})
    # Block until all async messages are sent
    producer.flush()
    print("FROM STOCK PRODUCER: Sent stock data")
    time.sleep(30)
    
    
