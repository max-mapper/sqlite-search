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

function filter() {
  var stream = through.obj(function(obj, enc, next) {
    if (obj.name && obj.readme) stream.push(obj)
    next()
  })
  return stream
}