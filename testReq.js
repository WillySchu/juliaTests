const fs = require('fs');
const iClient = require('./insightClient');
ic = new iClient('queue');

let res = fs.readFileSync('./testData.json', 'utf8');
res = JSON.parse(res);

ic.send('queue', res, (o) => {
  console.log(o.payload);
  console.log('done');
  process.exit(0)
})
