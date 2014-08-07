var http = require('http');
var JSON = require('JSON');

function putES(_path,data){
  var put_data = JSON.stringify(data);
  // console.log(put_data);

  var put_options = {
      host: _host,
      port: _port,
      path: _path,
      method: 'PUT',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': put_data.length
      }
  };

  var put_req = http.request(put_options, function(res) {
      res.setEncoding('utf8');
      res.on('data', function(data) {
        console.log(data);
      });
  });

  put_req.write(put_data);
  put_req.end();
}


function postES (_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_getTargetDataFun) {
  var post_data = 
  '{ \
    "query": { \
        "filtered": { \
          "query": { \
            "query_string": { \
              "query": "'+_sourceQuery+'" \
            } \
          }, \
          "filter": { \
            "bool": { \
              "must": [ \
                { \
                  "range": { \
                    "'+_sourceDate+'": { \
                      "from": 1356969600000 \
                    } \
                  } \
                } \
              ] \
            } \
          } \
        } \
      }, \
      "aggs":{ \
        "unique_user":{ \
          "date_histogram": { \
            "field": "'+_sourceDate+'", \
            "interval": "1M" \
          }, \
          "aggs":{ \
            "id":{ \
              "terms":{ \
                "field": "'+_sourceField+'", \
                "size": 1000 \
            } \
          } \
        } \
      } \
    } \
  }'; 
  
  var post_options = {
      host: _host,
      port: _port,
      path: _sourcePath,
      method: 'POST',
      headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          'Content-Length': post_data.length
      }
  };

  var post_req = http.request(post_options, function(res) {
      res.setEncoding('utf8');
      var responseString = '';

      res.on('data', function(data) {
        responseString += data;
      });

      res.on('end', function() {
        var resultObject = JSON.parse(responseString);
        // console.log(responseString);
        resultObject.aggregations.unique_user.buckets.forEach(function(entry) {
            putES(_targetPath + entry.key, _getTargetDataFun(entry));

        });
      });
  });

  post_req.write(post_data);
  post_req.end();
}

// ===================================
var _host = "sj1glm661.corp.adobe.com";
var _port = 9200;

// ===================================
var _sourcePath = '/udc/_search?pretty';
var _sourceQuery = '_type:ComponentUsageDataEntity';
var _sourceDate = 'collectDate';
var _sourceField = 'user';
var _targetPath = '/udc2/ComponentUsageDataEntityMonth/';
var _get_unique_user = function(entry) {
  var users = [];
  entry.id.buckets.forEach(function(user) {
    users.push(user.key);
  });
  var _d = {};
  _d.collectDate=entry.key;
  _d.number=entry.id.buckets.length;
  _d.users=users;
  return _d;
}

postES(_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_get_unique_user);

// ===================================
var _sourcePath = '/udc/_search?pretty';
var _sourceQuery = '_type:CCMAccountEntity';
var _sourceDate = 'collectDate';
var _sourceField = 'ldap';
var _targetPath = '/udc2/CCMAccountEntityMonth/';

postES(_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_get_unique_user);

// ===================================
var _sourcePath = '/subscription_email_trigger/_search?pretty';
var _sourceQuery = '*';
var _sourceDate = '@timestamp';
var _sourceField = 'userId';
var _targetPath = '/udc2/subscription_email_trigger_Month/';
var _get_unique_user = function(entry) {
  var users = [];
  entry.id.buckets.forEach(function(user) {
    users.push(user.key);
  });
  var _d = {};
  _d['@timestamp']=entry.key;
  _d['number']=entry.id.buckets.length;
  _d['users']=users;
  return _d;
}
postES(_sourcePath,_sourceQuery,_sourceDate,_sourceField,_targetPath,_get_unique_user);



