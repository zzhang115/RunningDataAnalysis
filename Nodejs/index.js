// // - get command line arguments
// // node index.js --port=8080 --redis_host=192.168.99.100 --redis_port=6379 --subscribe_channel=redis-stock-analyzer
// var argv = require('minimist')(process.argv.slice(2));
// var port = argv['port'];
// var redis_host = argv['redis_host'];
// var redis_port = argv['redis_port'];
// var subscribe_channel = argv['subscribe_channel'];
//
// // - setup dependency instance
// var express = require('express');
// var app = express();
// var server = require('http').createServer(app);
// var io = require('socket.io')(server);
//
// // - setup redis client
// var redis = require('redis');
// console.log('Creating redis client');
// var redisclient = redis.createClient(redis_port, redis_host);
// console.log('Subscribe to redis topic %s', subscribe_channel);
// redisclient.subscribe(subscribe_channel);
// redisclient.on('message', function(channel, message){
//     if(channel = subscribe_channel){
//         console.log('message received %s', message);
//         io.sockets.emit('data', message);
//     }
// });
//
// // - setup webapp routing
// app.use(express.static(__dirname + '/public'));
// app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
// app.use('/d3', express.static(__dirname + '/node_modules/d3/'));
// app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build/'));
// app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist/'));
// server.listen(port, function () {
//     console.log('Server started at %d', port)
// });
//
// // - setup shutdown hook
// var shutdown_hook = function(){
//     console.log('Quitting redis client');
//     redisclient.quit();
//     console.log('Shutting down app');
//     process.exit();
// };
//
// process.on('SIGTERM', shutdown_hook);
// process.on('SIGINT', shutdown_hook);
// process.on('exit', shutdown_hook);
// - get command line arguments
var argv = require('minimist')(process.argv.slice(2));
var port = argv['port'];
var redis_host = argv['redis_host'];
var redis_port = argv['redis_port'];
var subscribe_topic = argv['subscribe_topic'];

// - setup dependency instances
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);

// - setup redis client
var redis = require('redis');
console.log('Creating a redis client');
var redisclient = redis.createClient(redis_port, redis_host);
console.log('Subscribing to redis topic %s', subscribe_topic);
redisclient.subscribe(subscribe_topic);
redisclient.on('message', function (channel, message) {
    if (channel == subscribe_topic) {
        console.log('message received %s', message);
        io.sockets.emit('data', message);
    }
});

// - setup webapp routing
app.use(express.static(__dirname + '/public'));
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
app.use('/d3', express.static(__dirname + '/node_modules/d3/'));
app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build/'));
app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist'));

server.listen(port, function () {
    console.log('Server started at port %d.', port);
});

// - setup shutdown hooks
var shutdown_hook = function () {
    console.log('Quitting redis client');
    redisclient.quit();
    console.log('Shutting down app');
    process.exit();
};

process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);
