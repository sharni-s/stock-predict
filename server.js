const port = 3000;

const express = require("express");
const mqtt = require("mqtt");
const mongoose = require("mongoose");
const dotenv = require("dotenv");
dotenv.config();

const app = express();

// Body parser
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

// For loading static files
const base = `${__dirname}/public`;
app.use(express.static("public"));

// Connect to database
mongoose.connect(process.env.MONGO_URL, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Custom middleware that attaches response headers for cross-origin requests
app.use(function (req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header(
    "Access-Control-Allow-Headers",
    "Origin, X-Requested-With, Content-Type, Accept"
  );
  next();
});

// Connect to mqtt broker and subscribe to /stockData/tesla
const client = mqtt.connect("mqtt://broker.hivemq.com:1883");
client.on("connect", () => {
  console.log("Connected to MQTT broker");
  client.subscribe("/stockData/tesla");
});
client.on("error", (error) => {
  console.log(`Error Connecting to MQTT broker: ${error}`);
});

// mqtt publish -h broker.hivemq.com -p 1883 -t /stockData/tesla

const StockDataSchema = new mongoose.Schema({
  curr_timestamp: Number,
  curr_stockval: Number,
  next_timestamp: Number,
  next_stockval: Number,
});
const StockData = new mongoose.model("StockData", StockDataSchema);

client.on("message", (topic, message) => {
  recvJSON = JSON.parse(message.toString()); // message is Buffer. So, convert it to JSON format
  console.log("Received data = ", recvJSON);
  const newStockData = new StockData({
    curr_timestamp: new Date(recvJSON.curr_timestamp).getTime(),
    curr_stockval: recvJSON.curr_stockval,
    next_timestamp: new Date(recvJSON.next_timestamp).getTime(),
    next_stockval: recvJSON.next_stockval,
  });
  newStockData.save();
});

app.get("/", (req, res) => {
  res.sendFile(`${base}/index.html`);
});

app.get("/stock-data/:timestamp", (req, res) => {
  let ts = Number(req.params.timestamp);
  // console.log(ts)
  StockData.find({ curr_timestamp: ts })
    .then((response) => {
      // console.log(response);
      res.send(response);
    })
    .catch((error) => {
      res.send([]);
    });
});

app.post("/start-forecast", (req, res) => {
  client.publish("/startForecast", "start");
  res.send("DONE");
});

app.listen(port, () => {
  console.log(`Stock Forecast Server running on port ${port}`);
});
