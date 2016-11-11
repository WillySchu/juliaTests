const fs = require('fs');
const iClient = require('./insightClient');
ic = new iClient('queue');

const res = fs.readFileSync('./testData.json', 'utf8');

ic.send('queue', res, (o) => {
  console.log(o);
  console.log('done');
  process.exit(0)
})
