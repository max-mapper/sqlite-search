var sqlite3 = require('sqlite3').verbose()
var eos = require('end-of-stream')
var through = require('through2')

module.exports = function(opts, cb) {
  if (!opts.name) opts.name = "data_search"
  var db = new sqlite3.Database(opts.path, function(err) {
    if (err) cb(err)
    
    setup(db, opts, function(err) {
      if (err) return cb(err)
        
      cb(null, {
        createWriteStream: createWriteStream,
        createSearchStream: createSearchStream,
        db: db
      })
      
      function createSearchStream(searchOpts) {
        var field = searchOpts.field
        var query = searchOpts.query
        var order = searchOpts.order || field
        var since = searchOpts.since
        var limit = searchOpts.limit
        var select = searchOpts.select || ['*']
        var statement = searchOpts.statement || 
          "SELECT " + select.join(', ') +
          " FROM " + opts.name +
          " WHERE " + field +
          (since ? " > '" + since + "' AND " + field : '') +
          " MATCH '" + query + "'" + 
          "ORDER BY " + order +
          (limit ? "LIMIT " + limit : '') +
          ";"
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
          var keys = Object.keys(obj).sort()
          var vals = []
          var placeholders = []
          keys.forEach(function(k) {
            vals.push(obj[k])
            placeholders.push('?')
          })
          db.run("INSERT INTO " + opts.name + " (" + keys.join(', ') + ") VALUES (" + placeholders.join(', ') + ")", vals, function(err) {
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
    })
  })
}

function setup(db, opts, cb) {
  db.run("CREATE VIRTUAL TABLE IF NOT EXISTS " + opts.name + " USING FTS3(" + opts.columns.join(', ') + ");", cb)
}