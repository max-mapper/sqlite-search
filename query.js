// usage: node test.js readme foo
var search = require('./')
var fs = require('fs')

search({path: 'test.sqlite', columns: ["foo", "bar"]}, function(err, db) {
  if (err) console.error('err')
  var opts = {
    field: process.argv[2],
    query: process.argv[3]
  }
  var searchStream = db.createSearchStream(opts)
  searchStream.on('data', function(row) {
    console.log(JSON.stringify(row))
  })
  searchStream.on('error', function(err) {
    console.error('error', err)
  })
})