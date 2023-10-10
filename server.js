
const express = require('express');
const bodyParser = require('body-parser');
let path=require('path');


function main () {
  let app = express(); // Export app for other routes to use
  let handlers = require('./handlers/handler.js');
  //const port = process.env.PORT || 8000;
  const port= 9000;
  //app.use(express.static(__dirname + '/../frontend/build/'));
  app.use(bodyParser.urlencoded({ // Middleware
    extended: true
  }));
  app.use(bodyParser.json());
  app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header('Access-Control-Allow-Methods', 'PUT, POST, GET, DELETE, OPTIONS');
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, authorization");
  next();
});
  // Routes & Handlers
  app.get('/prob1', handlers.prob1);      //done
  app.get('/prob2',handlers.prob2);   //done
  app.get('/prob3', handlers.prob3);      //done
  app.get('/prob4',handlers.prob4);   //done
    


  app.get('/', function(req, res) {
    console.log(__dirname);
  res.sendFile(path.join(__dirname + '/build/index.html'));
});
  app.listen(port, () => console.log(`Server is listening on port: ${port}`));
}

main();