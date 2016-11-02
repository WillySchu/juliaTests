document.addEventListener("DOMContentLoaded", function(event) {
  var ws = new WebSocket('ws:0.0.0.0:8080')
  ws.onmessage = (e) => {
    console.log(e);
    readBlob(e.data).then(result => {
      console.log(result);
    })
  }
  var button = document.getElementById('button');
  button.onclick = (e) => {
    ws.send(2);
  }
});

function readBlob(blob) {
  return new Promise((resolve, reject) => {
    var result;
    var reader = new FileReader();
    reader.addEventListener('loadend', () => {
      resolve(reader.result)
    })
    reader.readAsBinaryString(blob)
  })
}
