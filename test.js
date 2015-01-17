var sqliteSearch = require('./')
var fs = require('fs')
var test = require('tape')
var ndjson = require('ndjson')
var rimraf = require('rimraf')
var eos = require('end-of-stream')
var concat = require('concat-stream')

function setupDatabase(cb) {
  var searchOpts = {
    path: 'test.sqlite',
    primaryKey: 'name',
    columns: ["readme", "name"]
  }
  rimraf('test.sqlite', function () {
    sqliteSearch(searchOpts, function(err, searcher) {
      if (err) console.error('err')
      var writer = searcher.createWriteStream()
      fs.createReadStream('test.ndjson').pipe(ndjson.parse()).pipe(writer)
      writer.on('finish', function() {
        cb(err, searcher)
      })
    })
  })
}

function getValues(searcher, opts, cb) {
  var values = []

  var searchStream = searcher.createSearchStream(opts)
  searchStream.on('data', function(row) {
    values.push(row)
  })

  eos(searchStream, function (err) {
    cb(err, values)
  })
}


test('test that search returns the right rows', function (t) {
  setupDatabase(function (err, searcher) {
    t.plan(2)

    var opts = {
      field: 'readme',
      query: 'Build Status'
    }

    getValues(searcher, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 7)
    })
  })
})


test('test since', function (t) {
  setupDatabase(function (err, searcher)  {
    t.plan(2)

    var opts = {
      field: 'readme',
      query: 'Build Status',
      since: 'AQ'
    }

    getValues(searcher, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 6)
    })
  })
})


test('test limit', function (t) {
  setupDatabase(function (err, searcher)  {
    t.plan(2)

    var opts = {
      field: 'readme',
      query: 'Build Status',
      limit: 1
    }

    getValues(searcher, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 1)
    })
  })
})


test('test since and limit', function (t) {
  setupDatabase(function (err, searcher)  {
    t.plan(3)

    var opts = {
      field: 'readme',
      query: 'Build Status',
      limit: 1,
      since: 'AQ'
    }

    getValues(searcher, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 1)
      var row = values[0]
      t.equals(row.name, 'Backbone.Chosen')
    })
  })
})


test('test offset', function (t) {
  setupDatabase(function (err, searcher)  {
    t.plan(6)

    var opts = {
      field: 'readme',
      query: 'Build Status',
      limit: 1,
      offset: 0
    }

    getValues(searcher, opts, function (err, values) {
      t.ifError(err)
      t.equals(values.length, 1)

      var opts = {
        field: 'readme',
        query: 'Build Status',
        limit: 1,
        offset: 1
      }

      getValues(searcher, opts, function (err, values2) {
        t.ifError(err)
        t.equals(values2.length, 1)
        t.notEquals(values2[0], values[0], 'with offset, should get a different value')
        t.true(values2[0].name > values[0].name, 'with offset, the new value should be greater')
      })
    })

  })
})

test('test search with object type formatting', function (t) {
  setupDatabase(function (err, searcher)  {
    t.plan(2)

    var searchOpts = {
      field: 'readme',
      query: 'Build Status',
      offset: 0,
      limit: 1,
      formatType: 'object'
    }
    var searchStream = searcher.createSearchStream(searchOpts)
    searchStream.pipe(concat(function (data) {
      var json = JSON.parse(data)
      t.equals(json.rows.length, 1, 'should return just 1 value')
      t.ok(json.next, 'has next')
    }))
  })
})
