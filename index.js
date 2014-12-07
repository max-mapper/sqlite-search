var sqlite3 = require('sqlite3').verbose()
var series = require('run-series')
var eos = require('end-of-stream')
var through = require('through2')

module.exports = function(cb) {
  var db = new sqlite3.Database('./data.sqlite', function(err) {
    if (err) cb(err)
    
    setup(db, function(err) {
      if (err) return cb(err)
        
      cb(null, {
        createWriteStream: createWriteStream,
        createSearchStream: createSearchStream,
        db: db
      })
      
      function createSearchStream(query) {
        db.each("SELECT * FROM data_search WHERE readme MATCH '" + query + "';", function onRow(err, row) {
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
          if (!obj.name || !obj.readme) return next()
          db.run("INSERT INTO data_search (name, readme) VALUES (?, ?)", [obj.name, obj.readme], function(err) {
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

function setup(db, cb) {
  var ops = [
    // function(cb) { db.run("CREATE TABLE IF NOT EXISTS data (name VARCHAR PRIMARY KEY, readme TEXT);", cb) },
    function(cb) { db.run("CREATE VIRTUAL TABLE IF NOT EXISTS data_search USING FTS3(name, readme);", cb) }
    // function(cb) { db.run("INSERT INTO data_search(data_search) VALUES('rebuild');", cb) }
  ]
   series(ops, function(errs) {
    cb(errs)
  })
}