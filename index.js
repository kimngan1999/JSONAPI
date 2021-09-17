//const client = require('node-impala')
import { createClient } from "node-impala";
import express from "express";
import bodyParser from 'body-parser';
import _  from "lodash";
var app = express();

app.use(bodyParser.json());

import timeserie from './series.json';
import countryTimeseries from './country-series.json';
import { type } from "os";

var now = Date.now();
var data =[];
var bigdata = [];
var groupbydata = [];
var parquet = [];


for (var i = timeserie.length -1; i >= 0; i--) {
  var series = timeserie[i];
  var decreaser = 0;
  for (var y = series.datapoints.length -1; y >= 0; y--) {
    series.datapoints[y][1] = Math.round((now - decreaser) /1000) * 1000;
    decreaser += 50000;
  }
}


const client = createClient();

client.connect({
  host: '172.29.65.197',
  port: 21000,
  resultType: 'json-array'
}).then(message => console.log("message",message))
.catch(error => console.debug("error",error));;

client.connection.on("connected", () => {
  console.log("Impala is connected!!!!!");
});
 
client.query('SELECT * FROM default.q20 limit 5;')
  .then(result =>{  data = result;})
  .catch(err => console.error("err",err))
  .done(() => client.close().catch(err => console.error(err)));

client.getResultsMetadata('SELECT * FROM default.q20 limit 5;')
  .then(metaData => console.log(metaData))
  .catch(err => console.error(err));

client.explain('SELECT * FROM default.q20 limit 5;')
  .then(explanation => console.log(explanation))
  .catch(err => console.error(err));


  client.query('SELECT * FROM default.q20 ;')
  .then(result =>{  bigdata = result;})
  .catch(err => console.error("err",err))
  .done(() => client.close().catch(err => console.error(err)));

client.getResultsMetadata('SELECT * FROM default.q20 ;')
  .then(metaData => console.log(metaData))
  .catch(err => console.error(err));

client.explain('SELECT * FROM default.q20 ;')
  .then(explanation => console.log(explanation))
  .catch(err => console.error(err));

client.query('SELECT __time, sum(call_duration_fractional) FROM default.q20 GROUP BY 1;')
  .then(result =>{ groupbydata = result;})
  .catch(err => console.error("err",err))
  .done(() => client.close().catch(err => console.error(err)));

client.getResultsMetadata('SELECT __time, sum(call_duration_fractional) FROM default.q20 GROUP BY 1;')
  .then(metaData => console.log(metaData))
  .catch(err => console.error(err));

client.explain('SELECT __time, sum(call_duration_fractional) FROM default.q20 GROUP BY 1;')
  .then(explanation => console.log(explanation))
  .catch(err => console.error(err));


  client.query('SELECT * FROM default.parquet_file ;')
  .then(result =>{  parquet = result;})
  .catch(err => console.error("err",err))
  .done(() => client.close().catch(err => console.error(err)));

client.getResultsMetadata('SELECT * FROM default.parquet_file ;')
  .then(metaData => console.log(metaData))
  .catch(err => console.error(err));

client.explain('SELECT * FROM default.parquet_file ;')
  .then(explanation => console.log(explanation))
  .catch(err => console.error(err));

app.get('/apis',(req, res)=>{
  res.json(data);
});
app.get('/bigdata',(req, res)=>{
  res.json(bigdata);
});
app.get('/groupByData',(req, res)=>{
  res.json(groupbydata);
});

app.get('/parquetFile',(req, res)=>{
  res.json(parquet);
});

var annotation = {
  name : "annotation name",
  enabled: true,
  datasource: "generic datasource",
  showLine: true,
}

var annotations = [
  { annotation: annotation, "title": "Donlad trump is kinda funny", "time": 1450754160000, text: "teeext", tags: "taaags" },
  { annotation: annotation, "title": "Wow he really won", "time": 1450754160000, text: "teeext", tags: "taaags" },
  { annotation: annotation, "title": "When is the next ", "time": 1450754160000, text: "teeext", tags: "taaags" }
];

var tagKeys = [
  {"type":"string","text":"Country"}
];

var countryTagValues = [
  {'text': 'SE'},
  {'text': 'DE'},
  {'text': 'US'}
];

var now = Date.now();
var decreaser = 0;
for (var i = 0;i < annotations.length; i++) {
  var anon = annotations[i];

  anon.time = (now - decreaser);
  decreaser += 1000000
}

var table =
  {
    columns: [{text: 'Time', type: 'time'}, {text: 'Country', type: 'string'}, {text: 'Number', type: 'number'}],
    rows: [
      [ 1234567, 'SE', 123 ],
      [ 1234567, 'DE', 231 ],
      [ 1234567, 'US', 321 ],
    ]
  };
  
function setCORSHeaders(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST");
  res.setHeader("Access-Control-Allow-Headers", "accept, content-type");  
}


var now = Date.now();
var decreaser = 0;
for (var i = 0;i < table.rows.length; i++) {
  var anon = table.rows[i];

  anon[0] = (now - decreaser);
  decreaser += 1000000
}

app.all('/', function(req, res) {
  setCORSHeaders(res);
  res.send('I have a quest for you!');
  res.end();
});

app.all('/search', function(req, res){
  setCORSHeaders(res);
  var result = [];
  _.each(timeserie, function(ts) {
    result.push(ts.target);
  });

  res.json(result);
  res.end();
});

app.all('/annotations', function(req, res) {
  setCORSHeaders(res);
  console.log(req.url);
  console.log(req.body);

  res.json(annotations);
  res.end();
});

app.all('/query', function(req, res){
  setCORSHeaders(res);
  console.log(req.url);
  console.log(req.body);

  var tsResult = [];
  let fakeData = timeserie;

  if (req.body.adhocFilters && req.body.adhocFilters.length > 0) {
    fakeData = countryTimeseries;
  }

  _.each(req.body.targets, function(target) {
    if (target.type === 'table') {
      tsResult.push(table);
    } else {
      var k = _.filter(fakeData, function(t) {
        return t.target === target.target;
      });

      _.each(k, function(kk) {
        tsResult.push(kk)
      });
    }
  });
 
  res.json(tsResult);
  res.end();
});

app.all('/tag[\-]keys', function(req, res) {
  setCORSHeaders(res);
  console.log(req.url);
  console.log(req.body);

  res.json(tagKeys);
  res.end();
});

app.all('/tag[\-]values', function(req, res) {
  setCORSHeaders(res);
  console.log(req.url);
  console.log(req.body);

  if (req.body.key == 'City') {
    res.json(cityTagValues);
  } else if (req.body.key == 'Country') {
    res.json(countryTagValues);
  }
  res.end();
});

app.listen(3600);
