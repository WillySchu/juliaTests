const Redis = require('redis');
const moment = require('moment');
const fs = require('fs');

const Envelope = require('./insightPayload');

const redis = Redis.createClient();
// const redisPubSub = Redis.createClient();

const callbackMap = {};

redis.on('message', (channel, o) => {
  redis.unsubscribe(channel);
  if (callbackMap[channel] === undefined) {
    console.log('Ignoring message on channel: ', channel);
  } else {
    console.log(o.slice(0,100));
    console.log(o.slice(o.length-101));
    callbackMap[channel](JSON.parse(o))
    delete callbackMap[channel];
  }
})

class InsightClient {
  constructor(queue) {
    this.queue = queue;
  }

  send(key, payload, callback) {
    console.log('Sending: ', key);

    var envelope = new Envelope();
    envelope.key = key;
    envelope.returnKey = Envelope.returnKeyPrefix + key;
    envelope.payload = payload;
    envelope.stampCreated = moment().toString();
    callbackMap[envelope.returnKey] = callback;
    console.log(JSON.stringify(envelope).length);
    fs.writeFile('envelope.json', JSON.stringify(envelope));
    redis.rpush(this.queue, JSON.stringify(envelope), (e, o) => {
      redis.subscribe(envelope.returnKey);
      console.log('pushed');
    })
  }
}

module.exports = InsightClient;
