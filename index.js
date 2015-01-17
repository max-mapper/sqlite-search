var sqlite3 = require('sqlite3').verbose()
var eos = require('end-of-stream')
var through = require('through2')
var debug = require('debug')('sqlite-search')
var formatData = require('format-data')
var qs = require('querystring')
var pump = require('pump')

module.exports = SqliteSearch

function SqliteSearch(opts, cb) {
  var self = this
  if (!(this instanceof SqliteSearch)) return new SqliteSearch(opts, cb)
  if (!opts.name) opts.name = "data_search"

  self.primaryKey = opts.primaryKey
  self.name = opts.name
  self.columns = opts.columns

  self.db = new sqlite3.Database(opts.path, function(err) {
    if (err) cb(err)

    create(self.db, opts, function(err) {
      if (err) return cb(err)

      cb(null, self)
    })
  })
}

SqliteSearch.prototype.createSearchStream = function (searchOpts) {
  var self = this

  var field = searchOpts.field
  var query = searchOpts.query
  var order = searchOpts.order || self.primaryKey
  var since = searchOpts.since
  var limit = parseInt(searchOpts.limit)
  var offset = parseInt(searchOpts.offset || 0)
  var select = searchOpts.select || ['*']
  var formatType = searchOpts.formatType || 'ndjson'

  var statement = searchOpts.statement ||
    "SELECT " + select.join(', ') +
    " FROM " + self.name +
    " WHERE " +
    (since ? self.primaryKey + " > '" + since + "' AND " + field : field) +
    " MATCH '" + query + "'" +
    " ORDER BY " + order +
    (limit ? " LIMIT " + limit : '') +
    (offset ? " OFFSET " + offset : '') +
    ";"

  debug('executing \n', statement)
  self.db.each(statement, function onRow(err, row) {
    if (err) return searchStream.destroy(err)
    searchStream.push(row)
  }, function done() {
    searchStream.end()
  })
  var searchStream = through.obj()

  if (formatType == 'object') {
    debug('formatting stream to ', formatType)

    var formatOpts = {
      format: formatType
    }

    if (limit) {
      var nextOpts = {
        offset: offset + limit,
        limit:  limit
      }

      formatOpts.suffix = ', "next": "?' + qs.stringify(nextOpts) + '"}'
    }

    var formatStream = formatData(formatOpts)

    searchStream.on('error', function (err) {
      formatStream.emit('error', err)
    })

    return pump(searchStream, formatStream)
  }
  else {
    return searchStream
  }
}

SqliteSearch.prototype.createWriteStream = function () {
  var self = this
  var writer = through.obj(function(obj, enc, next) {
    var keys = Object.keys(obj).sort().filter(function(k) { return self.columns.indexOf(k) > -1 })
    var vals = []
    var placeholders = []
    keys.forEach(function(k) {
      vals.push(obj[k])
      placeholders.push('?')
    })
    var statement = "INSERT INTO " + self.name + " (" + keys.join(', ') + ") VALUES (" + placeholders.join(', ') + ")"
    self.db.run(statement, vals, function(err) {
      if (err) writer.destroy(err)
      next()
    })
  })
  eos(writer, function(err) {
    if (err) console.error('writer error', err)
    console.log('closing')
  })

  return writer
}

function create(db, opts, cb) {
  db.run("CREATE VIRTUAL TABLE IF NOT EXISTS " + opts.name + " USING FTS3(" + opts.columns.join(', ') + ");", cb)
}
