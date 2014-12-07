var search = require('./')
var fs = require('fs')
var ndjson = require('ndjson')

search(function(err, db) {
  if (err) return console.error('err', err)
  console.log('importing test.ndjson...')
  var writer = db.createWriteStream()
  process.stdin.pipe(ndjson.parse()).pipe(writer)
  writer.on('finish', function() {
    db.db.close()
  })
})