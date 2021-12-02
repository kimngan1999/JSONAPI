//const client = require('node-impala')
import { createClient } from "node-impala";
import express from "express";
import bodyParser from 'body-parser';
import _  from "lodash";
var app = express();

app.use(bodyParser.json());

import { type } from "os";

var now = Date.now();
var data =[];
var bigdata = [];
var groupbydata = [];
var dataParquest = [];



const client = createClient();

client.connect({
  host: '172.29.65.197',
  port: 21000,
  resultType: 'json-array'
}).then(message => console.log("message", message))
.catch(error => console.debug("error",error));;

client.connection.on("connected", () => {
  console.log("Impala is connected!!!!!");
});


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


  app.all('/tag[\-]keys', function(req, res) {
    setCORSHeaders(res);
    console.log(req.url);
    console.log(req.body);
  
    res.json(tagKeys);
    res.end();
  });

//   client.query('SELECT * FROM default.parquet_tbl_hdfs ;')
//   .then(result =>{  dataParquest = result; console.log(result)})
//   .catch(err => console.error("err",err))
//   .done(() => client.close().catch(err => console.error(err)));

// client.getResultsMetadata('SELECT * FROM default.parquet_tbl_hdfs ;')
//   .then(metaData => console.log(metaData))
//   .catch(err => console.error(err));

// client.explain('SELECT * FROM default.parquet_tbl_hdfs ;')
//   .then(explanation => console.log(explanation))
//   .catch(err => console.error(err));

// app.get('/apis',(req, res)=>{
//   res.json(data);
// });
// app.get('/bigdata',(req, res)=>{
//   res.json(bigdata);
// });
// app.get('/groupByData',(req, res)=>{
//   res.json(groupbydata);

// });

// app.get('/parquetTable',(req, res)=>{
//   try {
//     res.status(200).json(dataParquest);
//     res.end();
//     }
//     // res.status(200).send("Success")
//    catch (err) {
//     res.status(500).send(err);
//   }
 
// });











app.listen(3600);

