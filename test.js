var search = require('./')
var fs = require('fs')
var ndjson = require('ndjson')

search(function(err, db) {
  if (err) console.error('err')
  db.createSearchStream(process.argv[2]).on('data', function(row) {
    console.log(JSON.stringify(row))
  })
})