const iClient = require('./insightClient');
ic = new iClient('queue');

ic.send('queue', 'hello', (o) => {
  console.log(o);
  console.log('done');
  process.exit(0)
})
