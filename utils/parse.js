// parse.js
// wraps yajl into functioning code
// not that im bitter

var yajl = require('yajl');
var _ = require('underscore');

module.exports = function(buffer, cb) {
  var out = [];
  var h = new yajl.Handle({allowMultipleValues: true});

  h.on('error', function(err) {
    cb(err, null);
  });

  var currentContext = {};
  var previousContext = [];
  var previousKeys = [];
  var currentArray = [];
  var inArrayContext = false;
  var currentKey = null;
  var newObj = false;

  h.on('startMap', function(e) {
    /*if (!inArrayContext) {
      // push the current context onto the previous context array
      previousContext.push(currentContext);
      perviousKeys.push(currentKey);
      inArrayContext = false;
    }*/
    console.log(flatmap({}, e));
  });

  h.on('endMap', function(e) {
    /*if (inArrayContext) {
      // if we are in an array, then push the object onto the array context
      currentArray.push(currentContext);
      currentContext = {};
    } else {
      // if we are not in an array context, then push the object to the previous key
      var parentContext = previousContext.pop();
      var parentKey = previousKeys.pop();
      // push the object onto the map
      parentContext[parentKey] = currentContext;
      currentContext = parentContext;
    }*/
    console.log(flatmap({}, e));

  });

  h.on('startArray', function(e) {
    //inArrayContext = true;
    console.log(flatmap({}, e));
  });

  h.on('endArray', function(e) {
    //currentContext[key] = currentArray;
    //currentArray = [];
    //inArrayContext = false;
    console.log(flatmap({}, e));
  });

  h.on('mapKey', function(e) {
    ///currentKey = key;
    //currentContext[key] = null;
    console.log(flatmap({}, e));
  });

  h.on('null', function(e) {
    // dont need to do anything
    console.log(flatmap({}, e));
  });

  h.on('boolean', function(e) {
    //currentContext[currentKey] = bool;
    console.log(flatmap({}, e));
  });

  h.on('string', function(e) {
    //currentContext[currentKey] = str;
    console.log(flatmap({}, e));
  });

  h.on('integer', function(e) {
    //currentContext[currentKey] = integer;
    console.log(flatmap({}, e));
  });

  h.on('double', function(e) {
    //currentContext[currentKey] = doub;
    console.log(flatmap({}, e));
  });

  h.parse(buffer.toString());

  h.completeParse();

};

function flatmap(h, e, prefix) {
  if (!prefix) {
    prefix = '';
  }
  _.each(e, function(v, k) {
    if (_.isObject(v)) {
      flatmap(h, v, prefix + k + '_');
    } else {
      var key = prefix + k;
      if (v === true) {
        h[key] = true;
      } else if (v === false) {
        h[key] = false;
      } else if (!_.isArray(v)) {
        h[key] = v;
      }
    }
  });
  return h;
}
/*
def flatmap(h, e, prefix = '')
  e.each do |k,v|
    if v.is_a?(Hash)
      flatmap(h, v, prefix+k+"_")
    else
      key = prefix+k
      next if !@keys.include? key

      case v
      when TrueClass then h[key] = 1
      when FalseClass then h[key] = 0
      else
        next if v.nil?
        h[key] = v unless v.is_a? Array
      end
    end
  end
  h
end
*/

module.exports(new Buffer('{"k1":[{"k2":["val"]}]}'));
