// archive.js
// reads a github archive file and returns a JSON object

var zlib = require('zlib');
var fs = require('fs');
var async = require('async');
var _ = require('underscore');
var path = require('path');
var buffer = require('buffer');

// takes in a path and parses it
function parseFile(path, cb) {
  fs.readFile(path, function(err, buffer) {
    if (err) {
      console.log(err);
      cb(err, null);
    } else {
      zlib.gunzip(buffer, function(err, data) {
        if (err) {
          cb(err);
        } else {
          var str = data.toString();

          // try new line split first, but that will fail for early data
          var arr = str.split('\n');
          var retVal = extractJSON(arr);

          if (retVal.parsed.length == 0) {
            console.log('file is old: ' + path);
            arr = str.split('}{');
            retVal = extractBadJSON(arr);
          }

          cb(null, retVal);
        }
      });
    }
  });
}
module.exports.parseFile = parseFile;

function extractJSON(arr) {
  var output = [];
  var errCount = 0;
  // grab the majority of the objects
  for (var i = 0; i < arr.length; i++) {
    // yeah, try in a for loop is not fast, but whatevs
    try {
      output.push(JSON.parse(arr[i]));
    } catch (err) {
      errCount++;
    }
  }
  return {parsed: output, dropped: errCount};
}

function extractBadJSON(arr) {
  var output = [];
  var errCount = 0;
  // grab the majority of the objects
  try {
    output.push(JSON.parse(arr[0] + '}'));
  } catch (err) {
    errCount++;
  }
  for (var i = 1; i < arr.length - 1; i++) {
    try {
      output.push(JSON.parse('{' + arr[i] + '}'));
    } catch (err) {
      errCount++;
    }
  }
  try {
    output.push(JSON.parse('{' + arr[arr.length - 1]));
  } catch (err) {
    errCount++;
  }
  return {parsed: output, dropped: errCount};
}

function parseFiles(paths, cb) {
  // queue up a set number of files at a time
  var out = [];
  var q = async.queue(function(task, callback) {
    parseFile(task, function(err, data) {
      out.push(data);
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

function reformatFile(fileName, target, cb) {
  var output = {};
  console.log('\nreformatFile:' + fileName);
  parseFile(fileName, function(err, data) {
    if (err) {
      console.log('failed to parse');
      cb(err, null);
    } else {
      var normed = {};
      for (var i = 0; i < data.parsed.length; i++) {
        var evt = data.parsed[i];
        normed = {
          type: evt.type,
          actor: (evt.actor_attributes || evt.actor),
          repo: (evt.repository || evt.repo),
          payload: evt.payload
        };
        if (normed.repo) {
          if (!output[normed.repo.id]) {
            output[normed.repo.id] = {};
          }
          if (!output[normed.repo.id][normed.type]) {
            output[normed.repo.id][normed.type] = [];
          }
          output[normed.repo.id][normed.type].push(normed);
        }
      }
      var name = path.basename(fileName);
      var outFile = target + name;
      saveFile(output, outFile, function(err, ack) {
        cb(err, ack);
      });
    }
  });
}

function saveFile(payload, path, cb) {
  zlib.gzip(new Buffer(JSON.stringify(payload)), function(err, data) {
    fs.writeFile(path, data, function(err, res) {
      if (err) {
        console.log('error saving file: ' + path);
        console.log(err);
      }
      cb(err, res);
    });
  });
}

function reformatFiles(dir, target, callback) {
  fs.readdir(dir, function(err, files) {
    async.eachSeries(files, function(f, cb) {
      reformatFile(path.join(dir, f), target, function(err, ack) {
        if (err) {
          console.log('failed on: ' + f);
          console.log(err);
        }
        cb();
      });
    }, callback);
  });
}

function seperateFiles(dir, target, callback) {
  fs.readdir(dir, function(err, files) {
    async.eachSeries(files, function(f, cb) {
      fs.readFile(path.join(dir, f), function(err, buffer) {
        if (err) {
          console.log(err);
          cb(err, null);
        } else {
          zlib.gunzip(buffer, function(err, data) {
            var obj = JSON.parse(data.toString());
            // remove any events that do not deal with a repo
            delete obj['undefined'];
            var ids = Object.keys(obj);

            async.eachSeries(ids, function(id, idCallback) {
              // save the files, take that ssd!
              var out = {};
              out[id] = obj[id];
              var fullTarget = path.join(target, id);

              // create the write dir
              fs.exists(fullTarget, function(status) {
                if (!status) {
                  fs.mkdir(fullTarget, function(err) {

                    // save the files
                    saveFile(out, path.join(fullTarget, f.replace('index-', '')), function(err, ack) {
                      idCallback(err, ack);
                    });
                  });
                } else {
                  saveFile(out, path.join(fullTarget, f.replace('index-', '')), function(err, ack) {
                    idCallback(err, ack);
                  });
                }
              });
            }, function() {
              console.log('all files altered');
              cb(null, f);
            });
          });
        }
      });
    }, callback);
  });
}

function compressByMonth(dir, target, callback) {
  fs.readdir(dir, function(err, files) {
    if (err) {
      throw err;
    }
    // create the groups of files that we are going to run in sync
    var groups = {};
    for (var i = 0; i < files.length; i++) {
      var arr = files[i].replace('index-', '').replace('.json.gz', '').split('-');
      // do it on the month
      var key = arr[0] + '-' + arr[1] + '-' + arr[2];
      if (!groups[key]) {
        groups[key] = [];
      }
      groups[key].push(files[i]);
    }
    var monthsToProcess = Object.keys(groups);
    // for each month
    async.eachSeries(monthsToProcess, function(month, monthCallback) {
      console.log('on month: ' + month);
      // have the repo keys
      var monthSnapShot = {};
      // and each file
      async.eachSeries(groups[month], function(file, fileCallback) {
        // get a timestamp for the stapshot
        var hourTimestamp = file.replace('index-', '').replace('.json.gz', '');
        fs.readFile(path.join(dir, file), function(err, buffer) {
          zlib.gunzip(buffer, function(err, data) {
            var obj = JSON.parse(data);
            // delete bad parse
            delete obj['undefined'];
            _.each(obj, function(repo, repoId) {
              if (!monthSnapShot[repoId]) {
                monthSnapShot[repoId] = {};
              }
              monthSnapShot[repoId][hourTimestamp] = repo;
            });
            // done with the file
            console.log('done with: ' + file);
            fileCallback();
          });
        });
      }, function() {
        saveMonthIntoSeperateFiles(monthSnapShot, month, target);
        // now save the snap shot
        monthCallback();
      });
    }, callback);
  });
}

function saveMonthIntoSeperateFiles(monthSnapShot, month, targetDir) {
  var objIds = Object.keys(monthSnapShot);
  var q = async.queue(function(id, cb) {
    var fullTarget = path.join(targetDir, id);
    var out = {};
    out[id] = monthSnapShot[id];

    fs.exists(fullTarget, function(status) {
      if (!status) {
        fs.mkdir(fullTarget, function(err) {
          // save the files
          saveFile(out, path.join(fullTarget, month + '.json.gz'), function(err, ack) {
            cb();
          });
        });
      } else {
        saveFile(out, path.join(fullTarget, month + '.json.gz'), function(err, ack) {
          cb();
        });
      }
    });
  }, 500);

  q.drain = function() {
    console.log('finished writing: ' + month);
  };

  q.push(objIds, function() {});
}

compressByMonth('../reformat', '../seperate', function(err, res) {
  console.log('done');
});
// NOOOPE
/*seperateFiles('../reformat', '../seperate', function(err, res) {
  console.log('done');
  console.log(err);
  console.log(res);
});*/

//reformatFiles('../raw/', '../reformat/index-', function() {
//  console.log('done');
//});

//reformatFile('../raw/2011-04-10-1.json.gz', '../reformat/', function(err, ack) {

//});

//reformatFile('../raw/2012-04-10-1.json.gz', '../reformat/', function(err, ack) {

//});
