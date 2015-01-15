var search = require('./')
var fs = require('fs')
var test = require('tape')
var ndjson = require('ndjson')
var rimraf = require('rimraf')


test('test that search returns the right rows', function (t) {
  rimraf('test.sqlite', function () {
    search({path: 'test.sqlite', columns: ["foo", "bar"]}, function(err, db) {
      if (err) console.error('err')
      var writer = db.createWriteStream()
      fs.createReadStream('test.ndjson').pipe(ndjson.parse()).pipe(writer)
      writer.on('finish', function() {
        t.plan(1)

        var opts = {
          field: 'foo',
          query: 'taco'
        }

        var searchStream = db.createSearchStream(opts)
        searchStream.on('data', function(row) {
          t.deepEquals(row, {bar: 'abc', foo: 'taco'})
        })

        searchStream.on('error', function(err) {
          t.ifError(err)
        })
      })
    })
  })
})
