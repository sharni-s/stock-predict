import json
import time
import datetime
import pandas as pd 
import numpy as np
import sys
import warnings
from kafka import KafkaConsumer
from kafka import KafkaProducer
from multiprocessing import cpu_count
from joblib import Parallel
from joblib import delayed
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error 
from math import sqrt

warnings.filterwarnings("ignore")

def rmse(test, predicted):
    return sqrt(mean_squared_error(test, predicted))
    

def evaluate_sarima_model(train, test, arima_order, seasonal_order):
    try:
        # Do not calcuate if both order and seasonal differencing is 0
        if (arima_order[1] + seasonal_order[1]) == 0:
            # Return a high value of RMSE so that it sits at the bottom of the list when sorted
            return 999999999, arima_order, seasonal_order, -1
    
        # Fit the SARIMAX model    
        model = SARIMAX(train, order = arima_order, seasonal_order = seasonal_order)
        model_fit = model.fit(disp = 0)
    
        # Get the next four forecasts
        preds = model_fit.get_forecast(steps = 4)
        error = rmse(test, preds.predicted_mean[:-1])
         
        return error, arima_order, seasonal_order, preds.predicted_mean[3]
    
    except Exception as e:
        # In case of convergence errors, non-invertible errors, etc.
        return 999999999, arima_order, seasonal_order, -1

class StockConsumer:
    def __init__(self, topic):
        # Consume earliest available messages, don't commit offsets
        KafkaConsumer(auto_offset_reset = 'earliest', 
                      value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                      enable_auto_commit = False) 
        
                      
        # To consume latest messages from the topic
        self.consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'])
        
        # To produce predicted stock values and rmse
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        self.producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        
        print(f"FROM NODE {sys.argv[1]}: Kafka Consumer Initialized")
        
        self.train = []
        self.test = []
        self.p_vals = np.arange(2, 5) 
        self.d_vals = [int(sys.argv[1])] 
        self.q_vals = np.arange(2, 5) 
        self.P_vals = np.arange(0, 1) 
        self.D_vals = np.arange(0, 1) 
        self.Q_vals = np.arange(0, 1) 
        self.m_vals = np.arange(5, 6)
        # 4 * 1 * 4 * 1 * 1 * 1 * 1 = 9 sets of parameters
        
        
    def getTrainData(self):
        ColumnNames = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
        # Create pandas dataframe
        df = pd.read_csv('TeslaStock.csv', names = ColumnNames, header = None, skiprows = 1,
                          comment = '#', sep = ',', encoding = 'iso-8859-1')
        
        # Get stock data for the last one week 
        df_train = df[df['timestamp'].between('2021-10-04', '2021-10-07')]
        
        # Get the close stock values from the dataframe as training data
        self.train = df_train['close'].tolist()
        
        
    def findBestFit(self):
        print("CPU COUNT = ", cpu_count())
        
        # Utilize all available cores using n_jobs = cpu_count()
        executor = Parallel(n_jobs = cpu_count(), backend='multiprocessing')
        print(f"FROM NODE {sys.argv[1]}: Finding Best Fit")
        all_results = []
        
        try:
            # Call function in a parallel manner
            tasks = (delayed(evaluate_sarima_model)(self.train[:-2], self.test, (p, d, q), (P, D, Q, m)) 
                                                    for m in self.m_vals for Q in self.Q_vals for D in self.D_vals for P in self.P_vals 
                                                    for q in self.q_vals for d in self.d_vals for p in self.p_vals)  
            results = executor(tasks)
            all_results.append(results)
            
        except Exception as e:
            print('Fatal Error....')
            
        all_results_sorted = []
        for tuple_list in all_results:
            for element in tuple_list:
                all_results_sorted.append(element)
                
        all_results_sorted.sort(key = lambda x: x[0])
            
        return all_results_sorted[0]
        
        
    
    def startProcess(self):
        print("FROM NODE " + sys.argv[1] + ": Process Started")
        for message in self.consumer:
            newStockData = json.loads(message.value)
            # Update test data
            self.test = self.train[-2:] + [newStockData['stockPrice']]
            best_result = self.findBestFit()
            self.train += [newStockData['stockPrice']]            
            self.producer.send('stockPrediction', {'timestamp': newStockData['timestamp'], 
                                                   'curStockPrice': newStockData['stockPrice'],
                                                   'rmse': best_result[0], 
                                                   'next_timestamp': newStockData['next_timestamp'],
                                                   'predStockPrice': best_result[3]})
            self.producer.flush()
            print("FROM NODE " + sys.argv[1] + ": BEST RMSE = ", best_result[0])



def main():
    stock_consumer = StockConsumer('stock')
    
    stock_consumer.getTrainData()
    stock_consumer.startProcess()
    
main()
    
    