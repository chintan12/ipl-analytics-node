process.env.NODE_ENV = 'test';

let HandlerGenerator=require('../handlers/handler.js');
let assert=require("assert");
let chai = require('chai');
let chaiHttp = require('chai-http');
let server = require('../testserver');
let should = chai.should();
let expect = chai.expect();
let request=require('supertest');

chai.use(chaiHttp);


//.set({ 'API-Key': 'foobar', Accept: 'application/json' })



describe('Preparing to test API', () => {
    /*
  * Test the /login route
  */
  describe('API testing started', () => {

      
     /*Register API testing*/


      it('it should test prob1 API', function(done) {
  
        /*valid registration*/

        request(server)
        .get('/prob1')
        .expect(function(res) {
      
        })
        .expect(200)
        .then(response => {
           // response.body.should.have.property("status");
           // assert(response.body.status,"success")
        	response.body.should.be.an('object');
        	response.body['2016'].should.be.an('object');
        	response.body['2017'].should.be.an('object');


           for(var prop in response.body){
        		for(var nprop in response.body[prop]){
        			response.body[prop][nprop].should.be.an('array');
        		}
        	}
          done();
        }).catch(err => {
          console.log(err);
          done(new Error(err));
        })


       });
      

      it('it should test prob2 API', function(done) {
        
       /*For Valid user*/

        request(server)
        .get('/prob2')
        .set('Accept', 'application/json')
        .expect(function(res) {
       
        })
        .expect(200)
        .then(response => {

        	response.body.should.be.an('object');
        	response.body['2016'].should.be.an('object');
        	response.body['2015'].should.be.an('object');
        	response.body['2014'].should.be.an('object');
        	response.body['2017'].should.be.an('object');
        	response.body['2013'].should.be.an('object');

        	for(var prop in response.body){
        		for(var nprop in response.body[prop]){
        			response.body[prop][nprop].should.be.an('object')
        			response.body[prop][nprop].should.have.property('count4')
        			response.body[prop][nprop].should.have.property('count6')
        			response.body[prop][nprop].should.have.property('total runs')
        		}
        	}

          // response.body.should.have.property("success");
          // response.body.should.have.property("message"); 
          // adminId=mongoose.Types.ObjectId(response.body.clientId);
          // console.log("------------------------signin response");
          // token=response.headers.authorization.slice(7,response.headers.authorization.length);
          // console.log(token);
          // console.log("--------------------signin response end");

          done();
        }).catch(err => {
          console.log(err);
          done(new Error(err));
        })

     
       });



    /*Test addPatient API*/


     it('it should test prob3 API',function(done){
           
          request(server)
             .get('/prob3')
             .expect(function(res){
             })
             .expect(200)
             .then(response => {
              
              response.body.should.be.an('object');
              for(var prop in response.body){
              	  response.body[prop].should.be.an('array');
              	for(var nprop in response.body[prop]){
              		response.body[prop][nprop].should.be.an('array');
              		response.body[prop][nprop][1].should.be.an('object');
              	}
              }
              // response.body.should.have.property("status");
              // assert(response.body.status,"success");
              // PgId=mongoose.Types.ObjectId(response.body.pgId);
              done();

             }).catch(err => {
               console.log(err);
               //done(new Error(err));
             })



     });


     
     /*editProfile*/

      it('it should test prob 4 API', function(done) {
      		this.timeout(25000);
        request(server)
        .get('/prob4')
        .expect(function(res) {
          //console.log(res);
        })
        .expect(200)
        .then(response => {
          response.body.should.be.an('object');
          for(var prop in response.body){
          	response.body[prop].should.be.an('object');
          	response.body[prop].should.have.property('netRunRate')
          	response.body[prop].should.have.property('team')
          }
          done();
        }).catch(err => {
          console.log(err);
          done(new Error(err));
        })
       
       }); 

      

  });

});