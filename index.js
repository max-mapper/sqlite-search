var sqlite3 = require('sqlite3').verbose()
var eos = require('end-of-stream')
var through = require('through2')
var debug = require('debug')('sqlite-search')
var formatData = require('format-data')
var qs = require('querystring')

SqliteSearch = {}

SqliteSearch.search = function(db, opts) {
  var searchOpts = {
    field: opts.column,
    query: opts.term,
    limit: parseInt(opts.limit),
    offset: parseInt(opts.offset || 0),
    select: opts.select || ["*"]
  }

  debug('search', searchOpts)

  var formatOpts = {
    format: "object"
  }

  if (searchOpts.limit) {
    var nextOpts = {
      offset: searchOpts.offset + searchOpts.limit,
      limit: searchOpts.limit
    }

    formatOpts.suffix = ', "next": "?' + qs.stringify(nextOpts) + '"}'
  }

  return db.createSearchStream(searchOpts).pipe(formatData(formatOpts))
}

SqliteSearch.setup = function(opts, cb) {
  if (!opts.name) opts.name = "data_search"
  var db = new sqlite3.Database(opts.path, function(err) {
    if (err) cb(err)

    create(db, opts, function(err) {
      if (err) return cb(err)

      var primaryKey = opts.primaryKey

      cb(null, {
        createWriteStream: createWriteStream,
        createSearchStream: createSearchStream,
        db: db
      })

      function createSearchStream(searchOpts) {
        var field = searchOpts.field
        var query = searchOpts.query
        var order = searchOpts.order || primaryKey
        var since = searchOpts.since
        var limit = searchOpts.limit
        var offset = searchOpts.offset
        var select = searchOpts.select || ['*']

        var statement = searchOpts.statement ||
          "SELECT " + select.join(', ') +
          " FROM " + opts.name +
          " WHERE " +
          (since ? primaryKey + " > '" + since + "' AND " + field : field) +
          " MATCH '" + query + "'" +
          " ORDER BY " + order +
          (limit ? " LIMIT " + limit : '') +
          (offset ? " OFFSET " + offset : '') +
          ";"

        debug('executing \n', statement)
        db.each(statement, function onRow(err, row) {
          if (err) return reader.destroy(err)
          reader.push(row)
        }, function done() {
          reader.end()
        })
        var reader = through.obj()
        return reader
      }

      function createWriteStream() {
        var writer = through.obj(function(obj, enc, next) {
          var keys = Object.keys(obj).sort().filter(function(k) { return opts.columns.indexOf(k) > -1 })
          var vals = []
          var placeholders = []
          keys.forEach(function(k) {
            vals.push(obj[k])
            placeholders.push('?')
          })
          var statement = "INSERT INTO " + opts.name + " (" + keys.join(', ') + ") VALUES (" + placeholders.join(', ') + ")"
          db.run(statement, vals, function(err) {
            if (err) writer.destroy(err)
            next()
          })
        })

        eos(writer, function(err) {
          if (err) console.error('writer error', err)
        })

        return writer
      }
    })
  })
}

function create(db, opts, cb) {
  db.run("CREATE VIRTUAL TABLE IF NOT EXISTS " + opts.name + " USING FTS3(" + opts.columns.join(', ') + ");", cb)
}

module.exports = SqliteSearch
