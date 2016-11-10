const fs = require('fs');
const iClient = require('./insightClient');
ic = new iClient('queue');

const res = fs.readFileSync('./mockGAres.js', 'utf8');

console.log(res);

ic.send('queue', res, (o) => {
  console.log(o);
  console.log('done');
  process.exit(0)
})
