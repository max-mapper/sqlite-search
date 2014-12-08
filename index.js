var sqlite3 = require('sqlite3').verbose()
var series = require('run-series')
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
        var select = searchOpts.select || ['*']
        var statement = "SELECT " + select.join(', ') + " FROM " + opts.name + " WHERE " + field + " MATCH '" + query + "';"
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
  var ops = [
    // function(cb) { db.run("CREATE TABLE IF NOT EXISTS data (name VARCHAR PRIMARY KEY, readme TEXT);", cb) },
    function(cb) { db.run("CREATE VIRTUAL TABLE IF NOT EXISTS " + opts.name + " USING FTS3(" + opts.columns.join(', ') + ");", cb) }
    // function(cb) { db.run("INSERT INTO data_search(data_search) VALUES('rebuild');", cb) }
  ]
   series(ops, function(errs) {
    cb(errs)
  })
}