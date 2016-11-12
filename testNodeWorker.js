const Redis = require('redis');
const Envelope = require('./insightPayload');

const redis = Redis.createClient();
// const redisPubSub = Redis.createClient();

const callbackMap = {};

redis.brpop('queue', 60, (err, o) => {
  s = o[1];
  console.log(o[0]);
  // console.log(Object.keys(s));
  console.log(s.returnKey);
  redis.publish('asdf', JSON.stringify(s))
})
