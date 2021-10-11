$("#start-btn").click(function () {
  $.ajax({
    type: "POST",
    url: `http://localhost:3000/start-forecast`,
    data: {},
    success: function (res) {
      console.log(res);

    },
  });
});

var data = [
  {
    x: [0],
    y: [0],
    mode: "scatter",
    name: "Actual Price",
    line: { color: "#80CAF6" },
  },
  {
    x: [0],
    y: [0],
    mode: "scatter",
    name: "Expected Price",
    line: { color: "#80BA46" },
  },
];

var layout = {
  autosize: true,
  title: "Tesla $TSLA Forecast",
  titlefont: { size: 25 },
  yaxis: {
    title: "Stock price",
    autorange: true,
    titlefont: { size: 20 },
  },
  xaxis: {
    title: "Time",
    autorange: true,
    titlefont: { size: 20 },
  },
};

var config = { responsive: true };

let curr_timestamp = 1633559700000;
// console.log(new Date(curr_timestamp))
// curr_timestamp = new Date(curr_timestamp).getTime();

var cnt = 0;

var interval = setInterval(function () {
  $.ajax({
    url: `http://localhost:3000/stock-data/${curr_timestamp}`,
    type: "GET",
    dataType: "json", // added data type
    success: function (res) {
      console.log(res[0]);
      if (res[0]) {
        console.log(curr_timestamp);
        if (document.querySelector("#graph:empty")) {
          data[0].x = [new Date(res[0].curr_timestamp)];
          data[0].y = [res[0].curr_stockval];
          data[1].x = [new Date(res[0].next_timestamp)];
          data[1].y = [res[0].next_stockval];
          Plotly.newPlot("graph", data, layout, config);
        } else {
          var update = {
            x: [
              [new Date(res[0].curr_timestamp)],
              [new Date(res[0].next_timestamp)],
            ],
            y: [[res[0].curr_stockval], [res[0].next_stockval]],
          };
          Plotly.extendTraces("graph", update, [0, 1]);
        }
        // console.log(curr_timestamp);
        // console.log("AFTER 5 MINUTES = ", new Date(curr_timestamp + 5 * 60000).getTime())
        // temp = new Date(curr_timestamp + 5 * 60000).getTime();
        curr_timestamp = new Date(res[0].next_timestamp).getTime();
        console.log("AFTER 5 MINUTES = ", curr_timestamp)
      }

      // if (++cnt === 100) clearInterval(interval);
    },
  });
}, 1000);
