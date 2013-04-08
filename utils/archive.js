// archive.js
// reads a github archive file and returns a JSON object

var zlib = require('zlib');
var fs = require('fs');
var async = require('async');
var _ = require('underscore');

// takes in a path and parses it
function parseFile(path, cb) {
  fs.readFile(path, function(err, buffer) {
    if (err) {
      console.log(err);
      cb(err, null);
    }
    zlib.gunzip(buffer, function(err, data) {
      if (err) {
        cb(err);
      } else {
        var str = data.toString();
        var arr = str.split('\n');
        var output = [];
        var errCount = 0;
        // grab the majority of the objects
        try {
          output.push(JSON.parse(arr[0]));
        } catch (err) {
          errCount++;
        }
        for (var i = 1; i < arr.length - 1; i++) {
          try {
            output.push(JSON.parse(arr[i]));
          } catch (err) {
            errCount++;
          }
        }
        try {
          output.push(JSON.parse(arr[arr.length - 1]));
        } catch (err) {
          errCount++;
        }
        cb(null, {parsed: output, dropped: errCount});
      }
    });
  });
}
module.exports.parseFile = parseFile;

function parseFiles(paths, cb) {
  // queue up a set number of files at a time
  var out = [];
  var q = async.queue(function(task, callback) {
    parseFile(task, function(err, data) {
      console.log('file parsed');
      console.log(data.parsed.length);
      out.push(data);
      console.log(out.length);
      callback();
    });
  }, 10);
  q.drain = function() {
    console.log('every thing is done');
    cb(null, out);
  };
  q.push(paths, function(err, data) {

  });

}
module.exports.parseFiles = parseFiles;
