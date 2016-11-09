'use strict';

var redis = require('redis')

var redisConf = {}
redisConf.host = "redis.db.metricstory.me";
redisConf.port = 6379;

client = redis.createClient()

client.set('key', 'val', () => {

})
