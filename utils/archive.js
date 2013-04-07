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
      cb(err, null);
    }
    zlib.gunzip(buffer, function(err, data) {
      if (err) {
        cb(err);
      } else {
        var str = data.toString();
        var arr = str.split('\n');
        console.log(arr.length);
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
  var q = async.queue(function(task, cb) {

  }, 100);
  async.map(paths, parseFile, function(err, data) {
    console.log(data);
    cb(err, data);
  });
}
module.exports.parseFiles = parseFiles;
