var search = require('./')
var fs = require('fs')
var ndjson = require('ndjson')
var through = require('through2')

search({path: 'test.sqlite', columns: ["foo", "bar"]}, function(err, db) {
  if (err) return console.error('err', err)
  var writer = db.createWriteStream()
  process.stdin.pipe(ndjson.parse()).pipe(writer)
  writer.on('finish', function() {
    db.db.close()
  })
})
