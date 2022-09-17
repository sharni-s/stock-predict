# Near Real-time Stock Value Prediction
Develop a real time stock forecasting system with time series forecasting. The model used for predicting the future stock values is the SARIMAX model. As stock values are continuously fluctuating, it hard to predict its value with a model that is not trained with the most up to date data. 
So, we train the SARIMAX model with a set of parameters whenever a new stock value is received. This way the predicted values are more accurate as the new model trained considers all the stock values. However, this is a time-consuming task, so we use distributed and parallel processing to get results faster. Once, the stock value is predicted it is plotted real time on the stock forecasting website for visualization. The following technologies will be implemented in this project,
-	Apache Kafka for sending and receiving data between the nodes (distributed system)
-	MQTT for publishing the results (predicted stock values)
-	Express.js Server to host the web server that serves the website which acts as the GUI for the user
-	Joblib python library for parallel processing
-	MongoDB for storing data
