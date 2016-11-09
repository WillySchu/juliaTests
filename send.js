'use strict';

const iClient = require('./insightClient.js');

const QUEUE_PREFIX = 'queuekey';
const insightClient = new iClient(QUEUE_PREFIX);

insightClient.send('stuff', 'stuff', (o) => {
  console.log(o);
})
