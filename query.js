// usage: node test.js readme foo
var search = require('./')
var fs = require('fs')

search({path: 'test.sqlite', columns: ["foo", "bar"]}, function(err, db) {
  if (err) console.error('err')
  db.createSearchStream(process.argv[2], process.argv[3]).on('data', function(row) {
    console.log(JSON.stringify(row))
  })
})