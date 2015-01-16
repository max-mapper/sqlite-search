var search = require('./')
var fs = require('fs')
var test = require('tape')
var ndjson = require('ndjson')
var rimraf = require('rimraf')
var eos = require('end-of-stream')

function setupDatabase(cb) {
  var searchOpts = {
    path: 'test.sqlite',
    primaryKey: 'name',
    columns: ["readme", "name"]
  }
  rimraf('test.sqlite', function () {
    search(searchOpts, function(err, db) {
      if (err) console.error('err')
      var writer = db.createWriteStream()
      fs.createReadStream('test.ndjson').pipe(ndjson.parse()).pipe(writer)
      writer.on('finish', function() {
        cb(err, db)
      })
    })
  })
}

function searchValues(db, opts, cb) {
  var values = []

  var searchStream = db.createSearchStream(opts)
  searchStream.on('data', function(row) {
    values.push(row)
  })

  eos(searchStream, function (err) {
    cb(err, values)
  })
}


test('test that search returns the right rows', function (t) {
  setupDatabase(function (err, db) {
    t.plan(2)

    var opts = {
      field: 'readme',
      query: 'Build Status'
    }

    searchValues(db, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 7)
    })
  })
})


test('test since', function (t) {
  setupDatabase(function (err, db)  {
    t.plan(2)

    var opts = {
      field: 'readme',
      query: 'Build Status',
      since: 'AQ'
    }

    searchValues(db, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 6)
    })
  })
})


test('test limit', function (t) {
  setupDatabase(function (err, db)  {
    t.plan(2)

    var opts = {
      field: 'readme',
      query: 'Build Status',
      limit: 1
    }

    searchValues(db, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 1)
    })
  })
})


test('test since and limit', function (t) {
  setupDatabase(function (err, db)  {
    t.plan(3)

    var opts = {
      field: 'readme',
      query: 'Build Status',
      limit: 1,
      since: 'AQ'
    }

    searchValues(db, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 1)
      var row = values[0]
      t.equals(row.name, 'Backbone.Chosen')
    })
  })
})
