// reduce.js
// compresses input github data into larger files that represent a longer timeframe

var archive = require('../utils/archive');
var fs = require('fs');
var _ = require('underscore');
var path = require('path');
var async = require('async');

// combines a set of data into a single json object
// also sorts based on type
function reduce(paths, callback) {
  var total = [];
  archive.parseFiles(paths, function(err, data) {
    console.log(err);
    console.log(data);
    var out = {};
    var dropped = 0;
    _.each(data, function(obj) {
      dropped += obj.dropped;
      _.each(obj.parsed, function(val, key) {
        if (!out[val.type]) {
          out[val.type] = [];
        }
        out[val.type].push(val);
      });
    });
    // now sort into types
    callback(null, out, dropped);
  });
}

function byDay(path, cb) {
  fs.readdir(path, function(err, files) {
    // now group the files by what we want
    var grouping = groupFileNames(files, path, 3);
    if (justOne) {
      var keys = Object.keys(grouping);
      var key = keys[keys.length - 1];
      tmp = {};
      tmp[key] = grouping[key];
      grouping = tmp;
      console.log(grouping);
    }
    var out = {};
    var counter = 0;
    _.each(grouping, function(fs, id) {
      counter++;
      reduce(fs, function(err, data, dropped) {
        counter--;
        out[id] = data;
        if (counter <= 0) {
          cb(null, out);
        }
      });
    });
  });
}

function byWeek(path, cb) {

}

function byMonth(path, cb) {
  fs.readdir(path, function(err, files) {
    // now group the files by what we want
    var grouping = groupFileNames(files, path, 2);
    var out = {};
    var counter = 0;
    _.each(grouping, function(fs, id) {
      counter++;
      reduce(fs, function(err, data, dropped) {
        counter--;
        out[id] = data;
        if (counter <= 0) {
          cb(null, out);
        }
      });
    });
  });
}

// fName - string to parse
// leftJoinCount - number of tokens to join in a string
function groupFileNames(files, root, numIndex) {
  var groups = {};
  _.each(files, function(f) {
    var name = path.basename(f);
    var date = name.split('-');
    // key for the day
    var key = '';
    for (var i = 0; i < numIndex; i++) {
      key += date[i];
      if (i + 1 < numIndex) {
        key += '-';
      }
    }
    if (!groups[key]) {
      groups[key] = [];
    }
    groups[key].push(path.join(root, f));
  });
  return groups;
}

byMonth('../raw/', function(err, data) {
  console.log(Object.keys(data));
});
